package callbacks

import (
	"bufio"
	"bytes"
	"deferredrequest/local/jsonout"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TIMEQUANTUMEXEC  time.Duration = 10 * time.Millisecond
	TIMEQUANTUMSTORE time.Duration = 10 * time.Second
	TIMEQUANTUMWAIT  time.Duration = 20 * time.Second
)

type Worker interface {
	Before(t *Task)
	After(t *Task)
}

type Task struct {
	Id      int64       `json:"id,omitempty"`      // id autoincrement, if len(name) == 0
	Name    string      `json:"name,omitempty"`    // name. if id ==0
	IsWork  bool        `json:"execute,omitempty"` // for named task only: true is task execute
	Created time.Time   `json:"created,omitempty"` // time create
	Timeout time.Time   `json:"timeout,omitempty"` // time for request callback
	URI     string      `json:"uri,omitempty"`     // callback URI
	Data    url.Values  `json:"data,omitempty"`    // callback data: GET if empty, POST otherwise.
	Rest    string      `json:"rest,omitempty"`    // callback readable rest: GET if empty, POST otherwise.
	Bin     []byte      `json:"bin,omitempty"`     // callback binart rest: GET if empty, POST otherwise.
	Headers http.Header `json:"header,omitempty"`  // callback binart rest: GET if empty, POST otherwise.
}

/*
Функция каллбек вызывается внешнешним менеджером, выполняет запрос в зависимости от полей структуры t:
может быть POST запрос со строковыми данными для len(t.Rest) != 0
может быть POST запрос с бинарными данными для len(t.Bin) != 0
может быть POST FORM запрос со строковыми данными для len(t.Data) != 0
может быть GET запрос

в начале запроса и по его окончании вызываются методы интерфейса w
*/
func (t *Task) Callback(w Worker, logger func(frm string, arg ...interface{})) {
	defer func() {
		if r := recover(); r != nil {
			_, file, line, _ := runtime.Caller(0)
			logger("Callback Recovered path=%s line=%d error=%v\n", file, line, r)
		}
	}()
	w.Before(t)
	defer w.After(t)
	client := http.Client{
		Timeout: TIMEQUANTUMWAIT,
	}
	var resp *http.Response = nil
	var respErr error = nil
	if len(t.Rest) != 0 {
		req, _ := http.NewRequest("POST", t.URI, bytes.NewBuffer([]byte(t.Rest)))
		for key, values := range t.Headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
		resp, respErr = client.Do(req)
	} else if len(t.Bin) != 0 {
		req, _ := http.NewRequest("POST", t.URI, bytes.NewBuffer(t.Bin))
		for key, values := range t.Headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
		resp, respErr = client.Do(req)
	} else if len(t.Data) != 0 {
		resp, respErr = client.PostForm(t.URI, t.Data)
	} else {
		resp, respErr = client.Get(t.URI)
	}
	if respErr != nil {
		logger("task: %d, uri: %s, request error: %s\n", t.Id, t.URI, respErr.Error())
		return
	}
	defer resp.Body.Close()

	body, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		logger("task: %d, uri: %s, read error: %s\n", t.Id, t.URI, errRead.Error())
	}

	logger("Exec task %d URI: %s responce %s", t.Id, t.URI, string(body))
}

type Callbacks struct {
	lastId    int64                                // for autoincrement
	tasks     []*Task                              // list of tasks sorted by request callback. ASC
	ids       map[int64]*Task                      // map of tasks for fast search by id
	namedIds  map[string]*Task                     // map of tasks for fast search by name
	mutex     sync.Mutex                           // mutex for sync
	work      int32                                // label for internal goroutine
	wg        *sync.WaitGroup                      // for clear exiting
	changed   bool                                 // label for change task
	storeFile string                               // file for store
	logger    func(frm string, arg ...interface{}) // external logger
}

// имплементируем интерфейс Worker
func (c *Callbacks) Before(t *Task) {
	// увеличиваем число глобальных задач
	c.wg.Add(1)
}

// имплементируем интерфейс Worker
func (c *Callbacks) After(t *Task) {
	// уменьшаем число глобальных задач
	c.wg.Add(-1)
	// если задача не именованная - выходим
	if len(t.Name) == 0 {
		return
	}
	// иначе восстанавливаем задачу
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// индекс в массиве
	index := c.getIndexByTask(t)
	// если задача удалена - выходим
	if index == -1 {
		return
	}
	// если задача не в статусе IsWork == true - т.е. ее удалили и создали заново с другим таймаутом
	if !c.tasks[index].IsWork {
		return
	}
	// иначе чистим массив
	c.tasks = append(c.tasks[:index], c.tasks[index+1:]...)

	// меняем задачу
	// убираем флаг IsWork
	t.IsWork = false
	// вычисляем старый таймаут
	duration := t.Timeout.Sub(t.Created) - 2*TIMEQUANTUMWAIT
	// устнавливаем новое время
	t.Created = time.Now()
	// и таймаут
	t.Timeout = t.Created.Add(duration)
	// вычисляем новое положение в массиве
	index = c.getIndexByTimeout(t.Timeout)
	// сперва добавляем в конец массива
	c.tasks = append(c.tasks, t)
	// смещаем остаток
	copy(c.tasks[index+1:], c.tasks[index:])
	// утснавливаем в нужное положение
	c.tasks[index] = t
	c.changed = true
}

/*
 * ищет задачу в массиве tasks по id или name
 * вернет индекс в этом массиве,
 * внимание поиск идет в отсортированном по Timeout массиве, поэтому время task.Timeout должно быть актуальным
 */
func indexByTask(tasks []*Task, task *Task) int {
	length := len(tasks)
	index := sort.Search(length, func(i int) bool { return tasks[i].Timeout.After(task.Timeout) })
	for i := index; i < length; i++ {
		if (task.Id != 0 && tasks[i].Id == task.Id) || (len(task.Name) != 0 && tasks[i].Name == task.Name) {
			return i
		}
		if !tasks[i].Timeout.Equal(task.Timeout) {
			break
		}
	}
	for i := index - 1; i >= 0; i-- {
		if (task.Id != 0 && tasks[i].Id == task.Id) || (len(task.Name) != 0 && tasks[i].Name == task.Name) {
			return i
		}
		if !tasks[i].Timeout.Equal(task.Timeout) {
			break
		}
	}
	return -1
}

/*
 * attantion
 * inner function - must call under lock
 * ищет задачу в массиве c.tasks по id или name
 * вернет индекс в этом массиве,
 * внимание поиск идет в отсортированном по Timeout массиве, поэтому время task.Timeout должно быть актуальным
 */
func (c *Callbacks) getIndexByTask(task *Task) int {
	return indexByTask(c.tasks, task)
}

/*
 * attantion
 * inner function - must call under lock
 * ищет место в  отсортированном по Timeout массиве с нужным timeout обыно нужно чтобы понять куда вставлять новый элемент
 */
func indexByTimeout(tasks []*Task, timeout time.Time) int {
	index := sort.Search(len(tasks), func(i int) bool { return tasks[i].Timeout.After(timeout) })
	return index
}

/*
 * attantion
 * inner function - must call under lock
 * ищет место в  отсортированном по Timeout массиве с нужным timeout обыно нужно чтобы понять куда вставлять новый элемент
 */
func (c *Callbacks) getIndexByTimeout(timeout time.Time) int {
	return indexByTimeout(c.tasks, timeout)
}

/*
 * берет первый элемент в отсортированном по Timeout массиве и если
 * время выполнения задачи истекло то выдает ее, если нет nil
 * для неименованного задания удаляет его из массива, для именнованного
 * перемещает задание гарантированно на место time.Now() + duration + 2*TIMEQUANTUMWAIT
 */
func (c *Callbacks) pop(maxTime time.Time) *Task {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(c.tasks) == 0 {
		return nil
	}
	res := c.tasks[0]
	if maxTime.Before(res.Timeout) {
		return nil
	}
	if len(res.Name) != 0 {
		// удалеяем первый элемент
		c.tasks = c.tasks[1:]
		// меняем статус IsWork
		res.IsWork = true
		// вычисляем старый duration
		duration := res.Timeout.Sub(res.Created)
		// обновляем время
		res.Created = time.Now()
		// таймаут ставим гарантированно больше времяни выполения запроса, но чтобы модно было вычилить старый duration
		res.Timeout = res.Created.Add(duration + 2*TIMEQUANTUMWAIT)
		// выясняем новое место в массиве
		index := c.getIndexByTimeout(res.Timeout)
		// сперва вставляем в конец
		c.tasks = append(c.tasks, res)
		// смещаем заднюю часть
		copy(c.tasks[index+1:], c.tasks[index:])
		// ставим в нужную позицию
		c.tasks[index] = res
	} else {
		c.tasks = c.tasks[1:]
		delete(c.ids, res.Id)
	}
	c.changed = true
	return res
}

func (c *Callbacks) check() {
	c.wg.Add(1)
	defer c.wg.Done()
	for atomic.LoadInt32(&c.work) != 0 {
		task := c.pop(time.Now())
		// если задач нет, спим
		if task == nil {
			time.Sleep(TIMEQUANTUMEXEC)
			continue
		}
		// если есть выполняем задачу, передаем c в качестве Worker интерфейса
		go task.Callback(c, c.logger)
	}
}

// запись массива в файл, построчно в формате json
func write(filePath string, array []*Task) error {
	file, errCreate := os.Create(filePath)
	if errCreate != nil {
		return errCreate
	}
	defer file.Close()
	delim := []byte("\n")
	for _, task := range array {
		bytes, errMar := json.Marshal(task)
		if errMar != nil {
			return errMar
		}
		file.Write(bytes)
		file.Write(delim)
	}
	return nil
}

// чтение массива в файл, построчно в формате json
func read(filePath string) ([]*Task, map[int64]*Task, map[string]*Task, int64) {
	file, errOpen := os.Open(filePath)
	if errOpen != nil {
		return make([]*Task, 0), make(map[int64]*Task), make(map[string]*Task), 0
	}
	defer file.Close()
	tasks := make([]*Task, 0)
	ids := make(map[int64]*Task)
	namedIds := make(map[string]*Task)
	var maxID int64 = 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		task := new(Task)
		if json.Unmarshal(scanner.Bytes(), task) == nil {
			tasks = append(tasks, task)
			if len(task.Name) != 0 { // для именнованных заданий инициализируем namedIds
				namedIds[task.Name] = task
			} else { // для неименнованных заданий инициализируем ids
				ids[task.Id] = task
			}
			if task.Id > maxID {
				maxID = task.Id
			}
		}
	}
	// проверка именнованных заданий на IsWork
	for _, task := range namedIds {
		if len(task.Name) != 0 && task.IsWork {
			index := indexByTask(tasks, task)
			if index == -1 {
				continue
			}
			// иначе чистим массив
			tasks = append(tasks[:index], tasks[index+1:]...)
			// меняем задачу
			// убираем флаг IsWork
			task.IsWork = false
			// вычисляем старый таймаут
			duration := task.Timeout.Sub(task.Created) - 2*TIMEQUANTUMWAIT
			// устнавливаем новое время
			task.Created = time.Now()
			// и таймаут
			task.Timeout = task.Created.Add(duration)
			// вычисляем новое положение в массиве
			index = indexByTimeout(tasks, task.Timeout)
			// сперва добавляем в конец массива
			tasks = append(tasks, task)
			// смещаем остаток
			copy(tasks[index+1:], tasks[index:])
			// утснавливаем в нужное положение
			tasks[index] = task
		}
	}
	return tasks, ids, namedIds, maxID
}

/*
 * сохранение на диск
 */
func (c *Callbacks) store() {
	c.wg.Add(1)
	defer c.wg.Done()
	for atomic.LoadInt32(&c.work) != 0 {
		if !c.IsChanged() {
			time.Sleep(TIMEQUANTUMSTORE)
			continue
		}
		f := false
		array := c.getList(0xEFFFFFFF, 0, &f)
		write(c.storeFile, array)
	}
}

func FileExists(filename string) int64 {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return -1
	}
	if info.IsDir() {
		return -2
	}
	return info.Size()
}

func NewCallbacks(wg *sync.WaitGroup, storeFile string, logger func(frm string, arg ...interface{})) *Callbacks {
	res := new(Callbacks)
	res.mutex = sync.Mutex{}
	res.work = 1
	res.wg = wg
	res.changed = false
	res.storeFile = storeFile
	res.logger = logger
	if len(res.storeFile) > 0 && FileExists(res.storeFile) >= 0 {
		res.tasks, res.ids, res.namedIds, res.lastId = read(res.storeFile)
		res.lastId = res.lastId + 1
		go res.store()
	} else {
		res.tasks = make([]*Task, 0)
		res.ids = make(map[int64]*Task)
		res.namedIds = make(map[string]*Task)
		res.lastId = 1
	}
	go res.check()
	return res
}

func (c *Callbacks) IsChanged() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.changed
}

// задания неизменяемые, если надо то сперва удаляем, потом вставляем
func (c *Callbacks) Set(add *Task) (*Task, int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(add.Name) > 0 { // если задание именованое
		task, ok := c.namedIds[add.Name]
		if ok {
			return task, c.getIndexByTask(task)
		}
	} else if add.Id != 0 { // если нет
		task, ok := c.ids[add.Id]
		if ok {
			return task, c.getIndexByTask(task)
		}
	} else {
		add.Id = c.lastId
		c.lastId++
	}
	index := c.getIndexByTimeout(add.Timeout)
	c.tasks = append(c.tasks, add)
	copy(c.tasks[index+1:], c.tasks[index:])
	c.tasks[index] = add
	if len(add.Name) > 0 {
		c.namedIds[add.Name] = add
	} else {
		c.ids[add.Id] = add
	}
	c.changed = true
	return add, index
}

// поиск по Id
func (c *Callbacks) GetById(id int64) *Task {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task, ok := c.ids[id]
	if !ok {
		return nil
	}
	return task
}

// поиск по Name
func (c *Callbacks) GetByName(name string) *Task {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task, ok := c.namedIds[name]
	if !ok {
		return nil
	}
	return task
}

// удаление по Id
func (c *Callbacks) DelById(id int64) (*Task, int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task, ok := c.ids[id]
	if !ok {
		return nil, -1
	}
	delete(c.ids, id)
	index := c.getIndexByTask(task)
	if index >= 0 {
		c.tasks = append(c.tasks[:index], c.tasks[index+1:]...)
		c.changed = true
	}
	return task, index
}

// удаление по Name
func (c *Callbacks) DelByName(name string) (*Task, int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task, ok := c.namedIds[name]
	if !ok {
		return nil, -1
	}
	delete(c.namedIds, name)
	index := c.getIndexByTask(task)
	if index >= 0 {
		c.tasks = append(c.tasks[:index], c.tasks[index+1:]...)
		c.changed = true
	}
	return task, index
}

// так как появились изменяемые именнованные задания то нужно делать глубокую копию
func (c *Callbacks) getList(limit int, offset int, isChange *bool) []*Task {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	res := make([]*Task, 0)
	length := len(c.tasks)
	for i := offset; i < offset+limit && i < length; i++ {
		tmp := *(c.tasks[i])
		res = append(res, &tmp)
	}
	if isChange != nil {
		c.changed = *isChange
	}
	return res
}

func (c *Callbacks) GetList(limit int, offset int) []*Task {
	res := c.getList(limit, offset, nil)
	for _, t := range res {
		if len(t.Name) != 0 && t.IsWork {
			// вычисляем старый таймаут
			duration := t.Timeout.Sub(t.Created) - 2*TIMEQUANTUMWAIT
			// ставим реальный таймаут
			t.Timeout = t.Created.Add(duration)
			// IsWork полезно не менять он может показыть что задание в работе
		}
	}
	return res
}

func (c *Callbacks) Stop() {
	atomic.StoreInt32(&c.work, 0)
	if len(c.storeFile) > 0 && c.IsChanged() {
		f := false
		array := c.getList(0xEFFFFFFF, 0, &f)
		write(c.storeFile, array)
	}
}

func parseDuration(durationStr string) (int64, error) {
	var duration int64 = 0
	var err error = nil
	switch {
	case strings.HasSuffix(durationStr, "ms"):
		_, err = fmt.Sscanf(durationStr, "%dms", &duration)
	case strings.HasSuffix(durationStr, "s"):
		_, err = fmt.Sscanf(durationStr, "%ds", &duration)
		duration = duration * 1000
	case strings.HasSuffix(durationStr, "min"):
		_, err = fmt.Sscanf(durationStr, "%dh", &duration)
		duration = duration * 1000 * 60
	case strings.HasSuffix(durationStr, "h"):
		_, err = fmt.Sscanf(durationStr, "%dh", &duration)
		duration = duration * 1000 * 3600
	default:
		_, err = fmt.Sscanf(durationStr, "%d", &duration)
		duration = duration * 1000
	}
	return duration, err
}

func parseHeaders(headers http.Header) http.Header {
	res := make(http.Header)
	for key, values := range headers {
		if !strings.HasPrefix(key, "X-Replace-") && !strings.HasPrefix(key, "x-replace-") {
			continue
		}
		for _, value := range values {
			res.Add(key[10:], value)
		}
	}
	return res
}

func (c *Callbacks) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			_, file, line, _ := runtime.Caller(0)
			c.logger("ServeHTTP Recovered path=%s line=%d error=%v\n", file, line, r)
		}
	}()
	c.wg.Add(1)
	defer c.wg.Done()
	for atomic.LoadInt32(&c.work) == 0 {
		jsonout.Response{Errno: jsonout.ServerError, Error: "Server is stopping", Data: nil}.PrintJson(w)
		return
	}
	var path string = ""
	if strings.HasSuffix(r.URL.Path, "/") {
		path = r.URL.Path[:len(r.URL.Path)-1]
	} else {
		path = r.URL.Path
	}
	name := ""
	if strings.HasPrefix(path, "/name/") {
		arr := strings.SplitN(path[6:], "/", 2)
		if len(arr) != 2 {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined name: /name/SomeName/put/...", Data: nil}.PrintJson(w)
			return
		}
		name = arr[0]
		path = "/" + arr[1]
	}
	if strings.HasPrefix(path, "/put/") {
		durationStr := path[5:]
		duration, errDur := parseDuration(durationStr)
		if errDur != nil {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined: /put/timeout", Data: nil}.PrintJson(w)
			return
		}
		errForm := r.ParseForm()
		if errForm != nil {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Parse Error", Data: nil}.PrintJson(w)
			return
		}
		uri, ok := r.Form["uri"]
		if !ok {
			jsonout.Response{Errno: jsonout.BadParam, Error: "'uri' parameter is absent", Data: nil}.PrintJson(w)
			return
		}
		task := new(Task)
		task.IsWork = false
		task.Name = name
		task.Id = 0 // add new task
		task.Headers = parseHeaders(r.Header)
		task.Created = time.Now()
		task.Timeout = task.Created.Add(time.Millisecond * time.Duration(duration))
		task.URI = uri[0]
		task.Data = r.PostForm
		delete(task.Data, "uri")
		res, _ := c.Set(task)
		c.logger("Add task %d URI: %s\n", res.Id, res.URI)
		jsonout.Response{Errno: jsonout.Ok, Error: "", Data: res}.PrintJson(w)
	} else if strings.HasPrefix(path, "/rest/") {
		arr := strings.SplitN(path[6:], "/", 2)
		if len(arr) != 2 {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined timeout & base64_uri: /rest/timeout/base64_full_uri", Data: nil}.PrintJson(w)
			return
		}
		duration, errDur := parseDuration(arr[0])
		if errDur != nil {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined timeout: /rest/timeout/base64_full_uri", Data: nil}.PrintJson(w)
			return
		}
		URL, errDecode := base64.StdEncoding.DecodeString(arr[1])
		if errDecode != nil {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined base64_uri: /rest/timeout/base64_full_uri", Data: nil}.PrintJson(w)
			return
		}
		body, errBody := io.ReadAll(r.Body)
		if errBody != nil {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined rest data: /rest/timeout/base64_full_uri", Data: nil}.PrintJson(w)
			return
		}
		defer r.Body.Close()
		task := new(Task)
		task.IsWork = false
		task.Name = name
		task.Id = 0 // add new task
		task.Headers = parseHeaders(r.Header)
		task.Created = time.Now()
		task.Timeout = task.Created.Add(time.Millisecond * time.Duration(duration))
		task.URI = string(URL[:])
		task.Rest = string(body[:])
		task.Headers = parseHeaders(r.Header)
		res, _ := c.Set(task)
		c.logger("Add task %d URI: %s\n", res.Id, res.URI)
		jsonout.Response{Errno: jsonout.Ok, Error: "", Data: res}.PrintJson(w)
	} else if strings.HasPrefix(path, "/bin/") {
		arr := strings.SplitN(path[6:], "/", 2)
		if len(arr) != 2 {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined timeout & base64_uri: /bin/timeout/base64_full_uri", Data: nil}.PrintJson(w)
			return
		}
		duration, errDur := parseDuration(arr[0])
		if errDur != nil {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined timeout: /bin/timeout/base64_full_uri", Data: nil}.PrintJson(w)
			return
		}
		URL, errDecode := base64.StdEncoding.DecodeString(arr[1])
		if errDecode != nil {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined base64_uri: /bin/timeout/base64_full_uri", Data: nil}.PrintJson(w)
			return
		}
		body, errBody := io.ReadAll(r.Body)
		if errBody != nil {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined rest data: /bin/timeout/base64_full_uri", Data: nil}.PrintJson(w)
			return
		}
		defer r.Body.Close()
		task := new(Task)
		task.IsWork = false
		task.Name = name
		task.Id = 0 // add new task
		task.Headers = parseHeaders(r.Header)
		task.Created = time.Now()
		task.Timeout = task.Created.Add(time.Millisecond * time.Duration(duration))
		task.URI = string(URL[:])
		task.Bin = body[:]
		res, _ := c.Set(task)
		c.logger("Add task %d URI: %s\n", res.Id, res.URI)
		jsonout.Response{Errno: jsonout.Ok, Error: "", Data: res}.PrintJson(w)
	} else if strings.HasPrefix(path, "/get/") {
		idStr := path[5:]
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined: /get/id", Data: nil}.PrintJson(w)
		} else {
			task := c.GetById(id)
			if task == nil {
				jsonout.Response{Errno: jsonout.NotFound, Error: "Not Found", Data: nil}.PrintJson(w)
			} else {
				jsonout.Response{Errno: jsonout.Ok, Error: "", Data: task}.PrintJson(w)
			}
		}
	} else if strings.HasPrefix(path, "/get") {
		if len(name) == 0 {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined: /name/SomeName/get", Data: nil}.PrintJson(w)
		}
		task := c.GetByName(name)
		if task == nil {
			jsonout.Response{Errno: jsonout.NotFound, Error: "Not Found", Data: nil}.PrintJson(w)
		} else {
			jsonout.Response{Errno: jsonout.Ok, Error: "", Data: task}.PrintJson(w)
		}
	} else if strings.HasPrefix(path, "/del/") {
		idStr := path[5:]
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined /del/id", Data: nil}.PrintJson(w)
		} else {
			task, _ := c.DelById(id)
			if task == nil {
				jsonout.Response{Errno: jsonout.NotFound, Error: "Not Found", Data: nil}.PrintJson(w)
			} else {
				c.logger("Del task %d URI: %s\n", task.Id, task.URI)
				jsonout.Response{Errno: jsonout.Ok, Error: "", Data: task}.PrintJson(w)
			}
		}
	} else if strings.HasPrefix(path, "/del") {
		if len(name) == 0 {
			jsonout.Response{Errno: jsonout.BadParam, Error: "Undefined: /name/SomeName/del", Data: nil}.PrintJson(w)
		}
		task, _ := c.DelByName(name)
		if task == nil {
			jsonout.Response{Errno: jsonout.NotFound, Error: "Not Found", Data: nil}.PrintJson(w)
		} else {
			c.logger("Del task %d URI: %s\n", task.Id, task.URI)
			jsonout.Response{Errno: jsonout.Ok, Error: "", Data: task}.PrintJson(w)
		}
	} else if strings.HasPrefix(path, "/list") {
		limit, errLim := strconv.Atoi(r.FormValue("limit"))
		if errLim != nil {
			limit = 10
		}
		offset, errOff := strconv.Atoi(r.FormValue("offset"))
		if errOff != nil {
			offset = 0
		}
		jsonout.Response{Errno: jsonout.Ok, Error: "", Data: c.GetList(limit, offset)}.PrintJson(w)
	} else {
		jsonout.Response{Errno: jsonout.BadParam, Error: "Bad Parameters", Data: nil}.PrintJson(w)
	}
}
