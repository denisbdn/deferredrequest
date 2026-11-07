# Этап 1: Сборка бинарного файла
FROM docker.io/library/golang:1.25.3 AS builder

WORKDIR /app

# Сначала копируем только файлы модулей и папку зависимостей
COPY go.mod go.sum ./

# Копируем остальной исходный код (сработает быстро при изменении кода)
COPY . .

# Собираем бинарник в режиме (Docker работает полностью оффлайн!)
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o deferredrequest main.go

# Этап 2: Финальный образ
FROM docker.io/library/golang:1.25.3-alpine AS release

WORKDIR /root/

# Копируем скомпилированный файл из этапа builder
COPY --from=builder /app/deferredrequest .

# Создаем директорию для хранения логов/файла задач
RUN mkdir -p /root/data

# Открываем порт сервиса
EXPOSE 1122

# Команда запуска
ENTRYPOINT ["./deferredrequest"]
CMD ["-addr", ":1122"]