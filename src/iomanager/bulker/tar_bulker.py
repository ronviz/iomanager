import tarfile
from io import BytesIO
from typing import override

from iomanager.bulker.bulker import Bulker, PackingError, SplittingError
from iomanager.message import Message
from iomanager.transaction.noop_transaction import NoopTransaction


class TarBulker(Bulker):
    @override
    def pack(self, messages: list[Message]) -> Message:
        try:
            buffer = BytesIO()
            with tarfile.open(
                fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT
            ) as tar:
                for i, message in enumerate(messages):
                    message.data.seek(0)

                    info = tarfile.TarInfo(name=str(i))
                    info.size = len(message.data.getbuffer().nbytes)
                    info.pax_headers = message.metadata.copy()

                    tar.addfile(tarinfo=info, fileobj=message.data)
            return Message(
                data=buffer,
                metadata={},
                transaction=NoopTransaction,
            )
        except Exception as e:
            raise PackingError("Could not pack message as into tar") from e

    @override
    def split(self, message: Message) -> list[Message]:
        message.data.seek(0)
        messages: list[Message] = []
        try:
            with tarfile.open(fileobj=message.data, mode="r") as tar:
                for member in tar.getmembers():
                    if not member.isfile():
                        continue

                    extracted = tar.extractfile(member)
                    if extracted is not None:
                        messages.append(
                            Message(
                                data=BytesIO(extracted.read()),
                                metadata=dict(member.pax_headers),
                                transaction=NoopTransaction,
                            )
                        )

            return messages
        except Exception as e:
            raise SplittingError("Could not split bulk") from e
