package app.exceptions

class MissingFieldException(id: String, field: String):
        Exception("Missing field '$field' in record '$id'.")
