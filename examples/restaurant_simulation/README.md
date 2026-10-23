# Restaurant Simulation (RabbitMQ)

Mikroservislar zanjiri namunasi:

```text
order_creator ──► chef_queue ──► chef_service ──► notification_queue
                                                        │
                                  delivery_service ◄── notification_service
```

Bildirishnoma (30%) va yetkazib berish (20%) bosqichlarida xatolik simulyatsiya qilinadi, xabar `nack(requeue=True)` bilan qayta navbatga qo'yiladi.

Ishga tushirish: asosiy [README](../../README.md) dagi "Qo'shimcha namuna" bo'limiga qarang.
