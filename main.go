package main

import (
	"context"
	"deferredrequest/local/callbacks"
	"deferredrequest/local/logs"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// конфиг
type _config struct {
	addr      *string
	storeFile *string
	maxproc   *int
	verbocity *int
}

var (
	config _config // глобальный конфиг
	logger *logs.Logs
)

// Обработчик событий сигналов
func signal_process(sigs chan os.Signal, exit_ch chan int) {
	for {
		sig := <-sigs
		logger.Errprint("SIGNAL_HANDLER: %s\n", sig.String())
		switch sig {
		case syscall.SIGPIPE: //0x0d == 13
		case syscall.SIGINT: //0x02 == 2
			logger.Errprint("SIGNAL_HANDLER: Exit")
			exit_ch <- 1
		case syscall.SIGTERM: //0x0f == 15
			logger.Errprint("SIGNAL_HANDLER: Exit")
			exit_ch <- 1
		default:
			logger.Errprint("SIGNAL_HANDLER: Unknown signal!")
		}
	}
}

func main() {
	config.addr = flag.String("addr", ":1122", "server bind addres")
	config.storeFile = flag.String("store", "", "store file")
	config.maxproc = flag.Int("maxproc", runtime.NumCPU()*2+1, "maximum process")
	config.verbocity = flag.Int("verb", 0, "verbocity")
	flag.Parse()

	logger = logs.NewLogs(*config.verbocity, log.New(os.Stdout, " out: ", 0), log.New(os.Stderr, " err: ", 0))
	logger.Errprintln("Server Start")
	logger.Errprintln("\taddr", *config.addr)
	logger.Errprintln("\tstore", *config.storeFile)
	logger.Errprintln("\tverbocity", *config.verbocity)
	logger.Errprintln("\tmaxproc", *config.maxproc)
	defer logger.Errprintln("Server Stop")

	// устанавливаем максимальное кол-во процессов
	runtime.GOMAXPROCS(*config.maxproc)

	// файл с бекапом
	if len(*config.storeFile) > 0 && callbacks.FileExists(*config.storeFile) == -1 {
		_, errCreate := os.Create(*config.storeFile)
		if errCreate != nil {
			logger.Errprint("Create file error: %s\n", errCreate.Error())
			return
		}
	}

	wg := sync.WaitGroup{}
	httpHandler := callbacks.NewCallbacks(&wg, *config.storeFile, func(frm string, arg ...interface{}) { logger.Logprint(frm, arg...) })
	mux := http.NewServeMux()
	mux.Handle("/", httpHandler)
	server := &http.Server{Addr: *config.addr, Handler: mux}
	go server.ListenAndServe()

	// канал выхода
	exit_ch := make(chan int)

	// обработчик сигналов
	sigs := make(chan os.Signal, 1)                                       // создание канала для приема сигналов
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGPIPE) // регистрация канала для сигналов
	go signal_process(sigs, exit_ch)

	// висим пока кто-то не запишет флаг в канал
	<-exit_ch

	httpHandler.Stop()

	// Даем серверу 10 секунд на завершение текущих обработок
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// завершаем веб сервер
	server.Shutdown(shutdownCtx)
	// server.Shutdown(context.Background()) // нет лимита по timeout

	// ожидаем завершения записи на диск (обработчики уже должны были завершиться)
	wg.Wait()
}
