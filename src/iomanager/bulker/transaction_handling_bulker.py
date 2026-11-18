from typing import override

from iomanager.bulker.bulker import Bulker
from iomanager.message import Message
from iomanager.transaction.transaction_stack import TransactionStack


class TransactionHandlingBulker(Bulker):
    def __init__(self, bulker: Bulker):
        self._bulker = bulker

    @override
    def pack(self, messages: list[Message]) -> Message:
        transaction_stack = TransactionStack()
        for message in messages:
            transaction_stack.push_transaction(message.transaction)

        message = self._bulker.pack(messages)
        message.transaction = transaction_stack
        return message

    @override
    def split(self, message: Message) -> list[Message]:
        new_messages = self._bulker.split(message)
        for new_message in new_messages:
            new_message.transaction = message.transaction

        return new_messages
