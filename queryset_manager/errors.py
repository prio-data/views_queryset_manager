
from views_schema import viewser as schema

TELL_AN_ADMIN = schema.Message(
            content = (
                "Tell and admin! If you requested a queryset when "
                "receiving this error, include it in your error "
                "report."
                ),
            message_type = schema.MessageType.HINT
        )

DEFAULT_ERROR_MESSAGES = {
        502: [
            schema.Message(
                content = (
                    "Queryset manager didn't manage to "
                    "an upstream resource."
                    ),
                message_type = schema.MessageType.MESSAGE
                ),
            TELL_AN_ADMIN
            ],
        500: [
            schema.Message(
                content = (
                    "Something went wrong upstream. "
                    "Queryset manager received a 500 error "
                    "without any further information."
                    ),
                message_type = schema.MessageType.MESSAGE
                ),
            TELL_AN_ADMIN
            ],
        202: [
            schema.Message(
                content = (
                    "The requested resource was pending."
                    ),
                message_type = schema.MessageType.MESSAGE
                ),
            schema.Message(
                content = (
                    "Try again later."
                    ),
                message_type = schema.MessageType.HINT
                )
            ]

    }
