class ZoekParameterOTL:
    def __init__(self, size: int = 100, from_cursor: str | None = None, expansion_field_list: list = None):
        self.size = size
        self.from_cursor = from_cursor
        self.expansion_field_list = expansion_field_list

    def to_dict(self):
        return {
            "size": self.size,
            "fromCursor": self.from_cursor,
            "expansions": {
                "fields": self.expansion_field_list
            }
        }