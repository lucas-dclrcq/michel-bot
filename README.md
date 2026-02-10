# michel-bot

A Matrix bot that helps manage the *Arr apps

## Features

- Receives *Arr webhook notifications and posts them to a Matrix room
- Tracks Seerr issues and manages them in Matrix

## Configuration

| Variable                | Required | Description                                                           |
|-------------------------|----------|-----------------------------------------------------------------------|
| `MATRIX_HOMESERVER_URL` | Yes      | Matrix homeserver URL                                                 |
| `MATRIX_USER_ID`        | Yes      | Bot's Matrix user ID                                                  |
| `MATRIX_PASSWORD`       | Yes      | Bot's Matrix password                                                 |
| `MATRIX_ROOM_ALIAS`     | Yes      | Room alias to post messages to                                        |
| `DATABASE_URL`          | Yes      | PostgreSQL connection string                                          |
| `SEERR_API_URL`         | Yes      | Seerr instance API URL                                                |
| `SEERR_API_KEY`         | Yes      | Seerr API key                                                         |
| `WEBHOOK_LISTEN_ADDR`   | No       | Listen address (default: `0.0.0.0:8080`)                              |
| `MATRIX_ADMIN_USERS`    | No       | Comma-separated list of Matrix user IDs allowed to run admin commands |

## Running with Docker

```sh
docker build -t michel-bot .

docker run -d \
  -e MATRIX_HOMESERVER_URL=https://matrix.example.com \
  -e MATRIX_USER_ID=@bot:example.com \
  -e MATRIX_PASSWORD=secret \
  -e MATRIX_ROOM_ALIAS='#room:example.com' \
  -e DATABASE_URL=postgres://user:pass@db/michel \
  -e SEERR_API_URL=https://seerr.example.com/api/v1 \
  -e SEERR_API_KEY=your-api-key \
  -p 8080:8080 \
  michel-bot
```

## Development

Prerequisites: Rust 1.88+, PostgreSQL, a Matrix homeserver, and a Seerr instance.

```sh
cargo build
cargo run
```

## Testing

Unit tests (no external dependencies):

```sh
cargo test --lib
```

Integration tests (requires Docker for testcontainers):

```sh
cargo test --test bdd
```

## Webhook endpoints

`POST /webhook/seerr` — receives Seerr webhook payloads.
