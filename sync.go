package middleware

import (
	"encoding/json"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Issue 结构体定义用户信息
type Issue struct {
	Id        int       `json:"id" gorm:"primaryKey"`
	Summary   string    `json:"Summary" gorm:"size:64"`
	Status    string    `json:"status" gorm:"size:64"`
	Di        int       `json:"di"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// IssueUpdate 结构体定义用户更新信息
type IssueUpdate struct {
	ID    int `json:"id"`
	Issue `json:"issue"`
}

// Config 结构体定义中间件的配置信息
type Config struct {
	DBHost        string
	DBPort        string
	DBUser        string
	DBPass        string
	DBName        string
	MQURL         string
	QueueName     string
	PrefetchCount int
}

// IssueUpdateMiddleware 结构体定义中间件
type IssueUpdateMiddleware struct {
	db     *gorm.DB
	conn   *amqp.Connection
	ch     *amqp.Channel
	config Config
}

// NewSyncMiddleware 函数用于创建新的中间件实例
func NewSyncMiddleware(config Config) (*IssueUpdateMiddleware, error) {
	dsn := config.DBUser + ":" + config.DBPass + "@tcp(" + config.DBHost + ":" + config.DBPort + ")/" + config.DBName + "?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	conn, err := amqp.Dial(config.MQURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &IssueUpdateMiddleware{
		db:     db,
		conn:   conn,
		ch:     ch,
		config: config,
	}, nil
}

// Start 方法用于启动中间件
func (m *IssueUpdateMiddleware) Start() error {
	err := m.ch.Qos(
		m.config.PrefetchCount, // 预取计数
		0,                      // 预取大小
		false,                  // 全局设置
	)
	if err != nil {
		return err
	}

	msgs, err := m.ch.Consume(
		m.config.QueueName, // 队列名称
		"",                 // 消费者标识（自动生成）
		false,              // 关闭自动确认
		false,              // 排他性
		false,              // 不等待
		false,              // 额外参数
		nil,
	)
	if err != nil {
		return err
	}

	forever := make(chan bool)

	go func() {
		for msg := range msgs {
			var update IssueUpdate
			if err := json.Unmarshal(msg.Body, &update); err != nil {
				log.Printf("JSON解析失败: %v", err)
				msg.Nack(false, false)
				continue
			}

			if err := m.db.Model(&Issue{}).Where("id = ?", update.ID).Updates(update.Issue).Error; err != nil {
				log.Printf("数据更新失败: %v", err)
				msg.Nack(false, false)
				continue
			}

			log.Printf("Issue id：%v 数据更新成功", update.ID)
			if err := msg.Ack(false); err != nil {
				log.Printf("消息确认失败: %v", err)
			}
		}
	}()

	log.Println("等待消息中...")
	<-forever
	return nil
}

// Close 方法用于关闭中间件
func (m *IssueUpdateMiddleware) Close() error {
	if err := m.ch.Close(); err != nil {
		return err
	}
	if err := m.conn.Close(); err != nil {
		return err
	}
	return nil
}
