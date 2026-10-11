FROM docker.io/library/golang:1.25.3 AS build

WORKDIR /app

# Копируем файлы зависимостей
COPY go.mod ./
# Если в репозитории есть go.sum, раскомментируйте строчку ниже:
# COPY go.sum ./ 
RUN go mod download

# Копируем исходный код
COPY . .

# Собираем оптимизированный бинарник под Linux
RUN CGO_ENABLED=0 GOOS=linux go build -o deferredrequest main.go

FROM docker.io/library/golang:1.25.3-alpine AS release

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Копируем собранный файл из прошлого этапа
COPY --from=builder /app/deferredrequest .

# Создаем директорию для хранения логов/файла задач
RUN mkdir -p /root/data

# Открываем порт, который сервис использует по умолчанию
EXPOSE 1122

# Команда запуска с флагами (сохранение в контейнере по пути /root/data/tasks.json)
ENTRYPOINT ["./deferredrequest", "-addr", ":1122", "-store", "/root/data/tasks.json"]
