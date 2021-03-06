package main

import (
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"time"

	models "github.com/eduardohitek/event-models"
	"github.com/fatih/color"
	"github.com/streadway/amqp"
	rabbitmq "github.com/wagslane/go-rabbitmq"
)

var sabores = []string{"Portuguese", "Calabresa", "4 Queijos", "Napolitana", "Frango", "Frango e Bacon"}
var tamanhos = []string{"P", "M", "G"}

func main() {
	color.Set(color.FgYellow, color.Bold)
	consumer := createConsumer("amqp://guest:guest@localhost")
	publisher := createPublisher("amqp://guest:guest@localhost")
	go startConsuming(consumer, "pedidos", "hifood", 10, "hifood.*", processEvent)
	go publishRandomPedidos(publisher)
	forever := make(chan struct{})
	<-forever
}

func startConsuming(consumer rabbitmq.Consumer, exchangeName string, queueName string,
	concurrencyNumber int, routingKey string,
	handler func(evento models.Evento)) {
	err := consumer.StartConsuming(
		func(d rabbitmq.Delivery) bool {
			evento, err := parseJSONToEvent(d.Body)
			if err != nil {
				log.Fatal(err)
			}
			handler(evento)
			return true
		},
		queueName,
		[]string{routingKey},
		rabbitmq.WithConsumeOptionsConcurrency(10),
		rabbitmq.WithConsumeOptionsQueueDurable,
		rabbitmq.WithConsumeOptionsBindingExchangeName(exchangeName),
		rabbitmq.WithConsumeOptionsBindingExchangeKind("topic"),
		rabbitmq.WithConsumeOptionsBindingExchangeDurable,
	)
	if err != nil {
		log.Fatal(err)
	}
}

func processEvent(evento models.Evento) {
	if evento.Tipo == "pedido-rejeitado" {
		color.Set(color.FgRed, color.Bold)
	}
	log.Printf("Pedido ID: %d - Resposta da cozinha: %s", evento.Pedido.SystemID, evento.Tipo)
	color.Set(color.FgYellow, color.Bold)
}

func publishRandomPedidos(publisher rabbitmq.Publisher) {
	for {
		pedido := generateRandomPedido()
		evento := models.Evento{Tipo: "pedido-criado", Pedido: pedido, Origem: "HiFood"}
		publishEvent(evento, "hifood.criar.pedido", "pedidos", publisher)
		time.Sleep(5 * time.Second)
	}
}

func createConsumer(url string) rabbitmq.Consumer {
	consumer, err := rabbitmq.NewConsumer(
		url, amqp.Config{},
		rabbitmq.WithConsumerOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	return consumer
}

func createPublisher(url string) rabbitmq.Publisher {
	publisher, _, err := rabbitmq.NewPublisher(
		url, amqp.Config{},
		rabbitmq.WithPublisherOptionsLogging,
	)
	if err != nil {
		log.Fatal(err)
	}
	return publisher
}

func parseJSONToEvent(eventJSON []byte) (models.Evento, error) {
	var evento models.Evento
	err := json.Unmarshal(eventJSON, &evento)
	return evento, err
}

func parseEventoToJSON(evento models.Evento) ([]byte, error) {
	eventoJSON, err := json.Marshal(evento)
	return eventoJSON, err
}

func publishEvent(evento models.Evento, publishKey string, exchangeName string, publisher rabbitmq.Publisher) {
	eventoBytes, err := parseEventoToJSON(evento)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Enviando o pedido: %s", string(eventoBytes))

	publish(publisher, eventoBytes, exchangeName, publishKey)
}

func publish(publisher rabbitmq.Publisher, evento []byte, exchangeName string, publishKey string) {
	err := publisher.Publish(
		evento,
		[]string{publishKey},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsMandatory,
		rabbitmq.WithPublishOptionsPersistentDelivery,
		rabbitmq.WithPublishOptionsExchange(exchangeName),
	)
	if err != nil {
		log.Fatal(err)
	}
}

func generateRandomPedido() models.Pedido {
	rand.Seed(time.Now().UTC().UnixNano())
	pedido := models.Pedido{SystemID: rand.Intn(999), Sabor: getRandomItem(sabores),
		Tamanho: getRandomItem(tamanhos), Valor: float32(math.Round(rand.Float64() * 100))}
	return pedido
}

func getRandomItem(options []string) string {
	randomIndex := rand.Intn(len(options))
	return options[randomIndex]
}
