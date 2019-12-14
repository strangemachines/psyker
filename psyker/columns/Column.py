# Copyright (C) 2019 Strangemachines
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# -*- coding: utf-8 -*-
from ..Sql import Sql
from ..exceptions import FieldTypeError


class Column:
    """
    The column of a table. Responsible for simplying the interaction
    with the Sql module.
    """

    __slots__ = ('name', 'field_type', 'nullable', 'unique', 'default',
                 'primary_key', 'options')

    def __init__(self, name, field_type, nullable=True, unique=False,
                 default=None, primary_key=False, **options):
        self.name = name
        self.field_type = field_type
        self.nullable = nullable
        self.unique = unique
        self.default = default
        self.primary_key = primary_key
        self.options = options

    def column_type(self):
        """
        Converts the column type to the corresponding Postgres type.
        """
        if self.field_type == 'int':
            return 'integer'
        elif self.field_type == 'float':
            return 'numeric'
        elif self.field_type == 'bool':
            return 'boolean'
        elif self.field_type == 'serial':
            return 'serial'
        elif self.field_type == 'str':
            if 'length' in self.options:
                length = self.options['length']
                return f'varchar({length})'
            return 'varchar'
        elif self.field_type == 'text':
            return 'text'
        elif self.field_type == 'date':
            return 'date'
        elif self.field_type == 'datetime':
            return 'timestamp'
        elif self.field_type == 'uuid':
            return 'uuid'
        elif self.field_type == 'foreign':
            reference_column = self.options['reference_column']
            return Sql.foreign_key(self.options['reference'], reference_column)
        raise FieldTypeError(self.field_type)

    def relationship(self):
        if self.field_type == 'foreign':
            return self.options['reference']

    def get_default(self):
        """
        Returns the default value. Not that in postgres all columns default to
        null when not specified.
        """
        if self.default:
            return self.default
        return ''

    def sql(self):
        """
        Returns the sql definition of the column
        """
        nullable = 'not null'
        unique = ''
        if self.nullable:
            nullable = ''
        if self.unique:
            unique = 'unique'
        return Sql.column(self.name, self.column_type(), nullable, unique,
                          self.get_default(), self.primary_key)

    def cast(self, value):
        """
        Casts a value to ensure it's of the correct type. Psycopg2 does most of
        the work, but some types are not supported, therefore when inserting
        extra conversions may be needed.
        """
        if self.field_type == 'uuid':
            return str(value)
        return value

    def __repr__(self):
        return f'<Column({self.name}, type: {self.field_type})>'
