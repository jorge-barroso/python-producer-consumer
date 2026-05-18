import csv
import logging

from confluent_kafka import Message

from messages.price.price_change import PriceChangeEvent


class PriceChangeMessageProcessor:
    def __init__(self):
        self.__f = open("price_change_message.json", "w")
        columns = PriceChangeEvent.model_fields.keys()
        self.__csv_writer = csv.DictWriter(self.__f, fieldnames=columns)
        self.__csv_writer.writeheader()


    def process_message(self, message: Message) -> None:
        """
        This is just a simple demo, specific business logic would typically start here
        Ideally making use of other service classes instead of this method
        to keep the system clean despite complexity and testable
        """
        log_message = f"""Message Received:
    Topic: {message.topic()}
    Partition: {message.partition()}    
    Offset: {message.offset()}"""
        logging.info(log_message)

        price_change = PriceChangeEvent.model_validate_json(message.value())
        data = price_change.model_dump()

        self.__csv_writer.writerow(data)

    def __del__(self):
        self.__f.close()