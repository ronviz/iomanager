import logging
from typing import override

from iomanager.bulker.bulker import Bulker, SplittingError
from iomanager.message import Message

BULKER_NAME_METADATA_KEY = "ADPATING_BULKER__BULKER_NAME"

logger = logging.getLogger(__name__)


class AdaptiveBulker(Bulker):
    """
    Gets a list of bulkers.
    Can unpack any bulk that was created by one of the internal bulkers.
    Packs using the defualt bulker
    """

    def __init__(
        self, bulkers: list[Bulker], defualt_bulker: Bulker, drop_unbulked_message: bool
    ):
        """
        :param drop_unbulked_message: If true, will drop message that were not bulked using AdaptingBulker
            else, will just return them as-is
        """
        self._name_to_bulker: dict[str, Bulker] = {
            self._get_bulker_name(bulker): bulker for bulker in bulkers
        }
        self.add_bulker(defualt_bulker)
        self._defualt_bulker_name = self._get_bulker_name(defualt_bulker)
        self._drop_unbulked_message = drop_unbulked_message

    @staticmethod
    def _get_bulker_name(bulker: Bulker) -> str:
        return str(type(bulker))

    def add_bulker(self, bulker: Bulker) -> None:
        self._name_to_bulker[self._get_bulker_name(bulker)] = bulker

    def set_defualt_bulker(self, bulker: Bulker):
        self._defualt_bulker_name = self._get_bulker_name(bulker)

        if self._defualt_bulker_name not in self._name_to_bulker:
            self.add_bulker(bulker)

    @override
    def pack(self, messages: list[Message]) -> Message:
        packed_message = self._name_to_bulker[self._defualt_bulker_name].pack(messages)
        packed_message.metadata[BULKER_NAME_METADATA_KEY] = self._defualt_bulker_name
        return packed_message

    @override
    def split(self, message: Message) -> list[Message]:
        original_bulker_name = message.metadata.get(BULKER_NAME_METADATA_KEY)
        if original_bulker_name is None:
            if self._drop_unbulked_message:
                logger.warning(
                    "Message was not bulked using 'AdaptingBulker'. Dropping"
                )
                raise SplittingError("Message was not bulked using 'AdaptingBulker")
            else:
                logger.warning(
                    "Message was not bulked using 'AdaptingBulker'. Assuming original message"
                )
                return [message]
        if original_bulker_name not in self._name_to_bulker:
            raise SplittingError(
                "Could not split message. The original bulker is not part of the 'AdaptingBulker' instance"
            )

        original_bulker = self._name_to_bulker[original_bulker_name]
        return original_bulker.split(message)
