// Package redis реализует usecase.Broker поверх Redis Streams.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Bananidze/rsufz/internal/domain"
	"github.com/Bananidze/rsufz/internal/usecase"
)

// Broker реализует usecase.Broker через Redis Streams.
type Broker struct {
	rdb *redis.Client
}

// New создаёт брокер с готовым клиентом Redis.
func New(rdb *redis.Client) *Broker {
	return &Broker{rdb: rdb}
}

// Publish добавляет task_id в Redis Stream (XADD). Вызывается планировщиком.
func (b *Broker) Publish(ctx context.Context, stream string, taskID domain.TaskID) error {
	err := b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"task_id": string(taskID)},
	}).Err()
	if err != nil {
		return fmt.Errorf("broker: publish to %s: %w", stream, err)
	}
	return nil
}

// Subscribe создаёт consumer group (если не существует) и возвращает канал доставки.
// Горутина читает из Redis Stream до закрытия ctx.
func (b *Broker) Subscribe(ctx context.Context, stream, group, consumer string) (<-chan usecase.Delivery, error) {
	// создаём consumer group (XGROUP CREATE ... MKSTREAM; $ = только новые сообщения)
	err := b.rdb.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return nil, fmt.Errorf("broker: create group %s/%s: %w", stream, group, err)
	}

	// при рестарте воркера сначала забираем pending-записи (XAUTOCLAIM)
	ch := make(chan usecase.Delivery, 64)
	go func() {
		defer close(ch)
		b.reclaimPending(ctx, stream, group, consumer, ch)
		b.readLoop(ctx, stream, group, consumer, ch)
	}()
	return ch, nil
}

// reclaimPending забирает сообщения, висящие в PEL дольше 30 секунд.
func (b *Broker) reclaimPending(ctx context.Context, stream, group, consumer string, ch chan<- usecase.Delivery) {
	msgs, _, err := b.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  30 * time.Second,
		Start:    "0-0",
		Count:    100,
	}).Result()
	if err != nil {
		return
	}
	for _, msg := range msgs {
		d, ok := parseDelivery(msg)
		if !ok {
			continue
		}
		select {
		case ch <- d:
		case <-ctx.Done():
			return
		}
	}
}

// readLoop читает новые сообщения через XREADGROUP с блокировкой.
func (b *Broker) readLoop(ctx context.Context, stream, group, consumer string, ch chan<- usecase.Delivery) {
	for {
		if ctx.Err() != nil {
			return
		}
		streams, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    10,
			Block:    2 * time.Second,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// временная ошибка Redis — пауза и повтор
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		for _, s := range streams {
			for _, msg := range s.Messages {
				d, ok := parseDelivery(msg)
				if !ok {
					continue
				}
				select {
				case ch <- d:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// Ack подтверждает обработку сообщения (XACK).
func (b *Broker) Ack(ctx context.Context, stream, group, msgID string) error {
	if err := b.rdb.XAck(ctx, stream, group, msgID).Err(); err != nil {
		return fmt.Errorf("broker: ack %s: %w", msgID, err)
	}
	return nil
}

func parseDelivery(msg redis.XMessage) (usecase.Delivery, bool) {
	raw, ok := msg.Values["task_id"]
	if !ok {
		return usecase.Delivery{}, false
	}
	id, ok := raw.(string)
	if !ok || id == "" {
		return usecase.Delivery{}, false
	}
	return usecase.Delivery{ID: msg.ID, TaskID: domain.TaskID(id)}, true
}
