package callbacks

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func buildTask(src *Task) *Task {
	res := new(Task)
	res.Id = src.Id
	res.Created = src.Created
	res.Timeout = src.Timeout
	res.URI = src.URI
	res.Data = src.Data
	return res
}

func TestCallbacksGetSetList(t *testing.T) {
	wg := sync.WaitGroup{}
	callbacks := NewCallbacks(&wg, "", func(frm string, arg ...interface{}) { fmt.Printf(frm, arg...) })
	now := time.Now()
	task := Task{Created: now, Timeout: now.Add(time.Hour), URI: "https://icanhazip.com/"}
	t1 := buildTask(&task)
	t1.URI = t1.URI + "?1"
	t2 := buildTask(&task)
	t2.URI = t1.URI + "?2"
	t2.Timeout = t2.Timeout.Add(time.Second)
	t3 := buildTask(&task)
	t3.URI = t1.URI + "?3"
	t3.Timeout = t2.Timeout.Add(time.Second)
	t4 := buildTask(&task)
	t4.URI = t1.URI + "?4"
	t4.Timeout = t2.Timeout.Add(time.Second)
	t5 := buildTask(&task)
	t5.URI = t1.URI + "?5"
	t5.Timeout = t2.Timeout.Add(2 * time.Second)
	t6 := buildTask(&task)
	t6.URI = t1.URI + "?6"
	t1Add, _ := callbacks.Set(t1)
	t2Add, _ := callbacks.Set(t2)
	t5Add, _ := callbacks.Set(t5)
	t3Add, _ := callbacks.Set(t3)
	t6Add, _ := callbacks.Set(t6)
	t4Add, _ := callbacks.Set(t4)
	t5AddDub, _ := callbacks.Set(t5Add)
	if t5Add != t5AddDub {
		t.Fatal("error add dublicate")
	}
	t6AddDub, _ := callbacks.Set(t6Add)
	if t6Add != t6AddDub {
		t.Fatal("error add dublicate")
	}

	prev := now
	list1 := callbacks.GetList(10, 0)
	if len(list1) != 6 {
		t.Fatal("wrong result length")
	}
	if len(callbacks.ids) != 6 {
		t.Fatal("wrong result length")
	}
	if len(callbacks.tasks) != 6 {
		t.Fatal("wrong result length")
	}
	for _, tmp := range list1 {
		// fmt.Println(i, tmp)
		if prev.After(tmp.Timeout) {
			t.Fatal("not sorted by Timeout")
		}
		prev = tmp.Timeout
	}

	callbacks.DelById(t2Add.Id)
	callbacks.DelById(t4Add.Id)
	callbacks.DelById(t1Add.Id)

	list2 := callbacks.GetList(10, 0)
	if len(list2) != 3 {
		t.Fatal("wrong result length")
	}
	if len(callbacks.ids) != 3 {
		t.Fatal("wrong result length")
	}
	if len(callbacks.tasks) != 3 {
		t.Fatal("wrong result length")
	}
	if list2[0].Id != t6.Id {
		t.Fatal("not sorted by Timeout")
	}
	if list2[1].Id != t3.Id {
		t.Fatal("not sorted by Timeout")
	}
	if list2[2].Id != t5.Id {
		t.Fatal("not sorted by Timeout")
	}

	list3 := callbacks.GetList(2, 1)
	if len(list3) != 2 {
		t.Fatal("wrong result length")
	}
	if list3[0].Id != t3.Id {
		t.Fatal("not sorted by Timeout")
	}
	if list3[1].Id != t5.Id {
		t.Fatal("not sorted by Timeout")
	}

	t3AddDub := callbacks.GetById(t3Add.Id)
	if t3Add.Id != t3AddDub.Id {
		t.Fatal("error get")
	}

	callbacks.Stop()
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-time.After(TIMEQUANTUMEXEC + time.Second):
		t.Fatal("Test didn't finish in time")
	case <-done:
	}
	t.Log("OK")
}

func TestCallbacksReadWriteFile(t *testing.T) {
	now := time.Now()
	task := Task{Created: now, Timeout: now.Add(time.Hour), URI: "https://icanhazip.com/"}
	t1 := buildTask(&task)
	t1.Id = 1
	t1.URI = t1.URI + "?1"
	t2 := buildTask(&task)
	t2.Id = 2
	t2.URI = t2.URI + "?2"
	t2.Timeout = t2.Timeout.Add(time.Second)
	array := []*Task{t1, t2}
	filePath := "task.json.log"
	errWrite := write(filePath, array)
	if errWrite != nil {
		t.Fatal(errWrite)
	}
	wg := sync.WaitGroup{}
	callbacks := NewCallbacks(&wg, filePath, func(frm string, arg ...interface{}) { fmt.Printf(frm, arg...) })

	t3 := buildTask(&task)
	t3.URI = t3.URI + "?3"
	t3.Timeout = t3.Timeout.Add(-1 * time.Second)
	callbacks.Set(t3)

	list1 := callbacks.GetList(10, 0)
	if len(list1) != 3 {
		t.Fatal("wrong result length")
	}
	if len(callbacks.ids) != 3 {
		t.Fatal("wrong result length")
	}
	if len(callbacks.tasks) != 3 {
		t.Fatal("wrong result length")
	}
	if list1[0].Id != t3.Id {
		t.Fatal("not sorted by Timeout")
	}
	if list1[1].Id != 1 {
		t.Fatal("bad read")
	}
	if list1[2].Id != 2 {
		t.Fatal("bad read")
	}

	c1 := callbacks.IsChanged()
	t.Logf("c1: %t", c1)

	callbacks.Stop()
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-time.After(TIMEQUANTUMSTORE + time.Second):
		t.Fatal("Test didn't finish in time")
	case <-done:
	}

	cb := NewCallbacks(&wg, filePath, func(frm string, arg ...interface{}) { fmt.Printf(frm, arg...) })
	list2 := cb.GetList(10, 0)
	if len(list1) != len(list2) {
		t.Fatal("wrong result length")
	}
	if len(callbacks.ids) != len(cb.ids) {
		t.Fatal("wrong result length")
	}
	if len(callbacks.tasks) != len(cb.tasks) {
		t.Fatal("wrong result length")
	}
	for i := 0; i < len(list1); i++ {
		if list1[i].Id != list2[i].Id ||
			!list1[i].Created.Equal(list2[i].Created) ||
			!list1[i].Timeout.Equal(list2[i].Timeout) ||
			list1[i].URI != list2[i].URI {
			t.Fatal("not equal")
		}
	}

	go func() {
		cb.Stop()
		wg.Wait()
		done <- true
	}()

	select {
	case <-time.After(TIMEQUANTUMSTORE + time.Second):
		t.Fatal("Test didn't finish in time")
	case <-done:
	}
	t.Log("OK")
}
