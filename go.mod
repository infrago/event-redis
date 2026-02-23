module github.com/bamgoo/event-redis

go 1.25.3

require (
	github.com/bamgoo/bamgoo v0.0.0-00010101000000-000000000000
	github.com/bamgoo/event v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.17.3
)

replace github.com/bamgoo/bamgoo => ../bamgoo

replace github.com/bamgoo/event => ../event
