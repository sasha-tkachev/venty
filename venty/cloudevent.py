import json
from typing import Dict, Any

from cloudevents.conversion import to_json
from cloudevents.pydantic import CloudEvent as _CloudEvent
from pydantic import model_serializer, BaseModel


class CloudEvent(_CloudEvent):
    @model_serializer(when_used="json")
    def _ce_json_dumps(self) -> Dict[str, Any]:
        """Performs Pydantic-specific serialization of the event when
        serializing the model using `.model_dump_json()` method.

        Needed by the pydantic base-model to serialize the event correctly to json.
        Without this function the data will be incorrectly serialized.

        :param self: CloudEvent.

        :return: Event serialized as a standard CloudEvent dict with user specific
        parameters.
        """
        # Here mypy complains about json.loads returning Any
        # which is incompatible with this method return type
        # but we know it's always a dictionary in this case
        to_serialize = self

        if isinstance(self.data, BaseModel):
            to_serialize = to_serialize.copy()
            to_serialize.data = json.loads(self.data.json(exclude_none=True))

        return json.loads(to_json(to_serialize))  # type: ignore
