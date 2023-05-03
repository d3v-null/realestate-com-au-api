from datetime import datetime, timedelta, date as dt_date

class DFMixin:
    def get_field_df(self, name):
        # value = self.__dict__[field.name]
        value = getattr(self, name)
        field = self.__dataclass_fields__.get(name)
        if field and value and 'datetime.date' in str(field.type):
            value = datetime(*value.timetuple()[:6])
        return value

    def get_row_df(self, columns=None):
        if not columns:
            columns = self.__dataclass_fields__.keys()
        # .__dataclass_fields__[name]
        return [self.get_field_df(name) for name in columns]