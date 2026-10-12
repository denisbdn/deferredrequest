# Этап 1: Сборка бинарного файла
FROM docker.io/library/golang:1.25.3 AS builder

WORKDIR /app

# Сразу копируем абсолютно ВСЕ файлы (включая скачанную папку vendor)
COPY . .

# Собираем бинарник в режиме -mod=vendor (Docker работает полностью оффлайн!)
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -a -installsuffix cgo -o deferredrequest main.go

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
ENTRYPOINT ["./deferredrequest", "-addr", ":1122", "-store", "/root/data/tasks.json"]