import struct
import json
import uuid
import sqlparse
import re
import datetime
from dataclasses import dataclass
from enum import Enum
from pyparsing import Suppress, CaselessKeyword, Word, alphas, alphanums, delimitedList
import copy

from .table_structure import TableStructure, TableField
from .enum import (
    parse_mysql_enum, EnumConverter,
    parse_enum_or_set_field,
    extract_enum_or_set_values
)
from .mysql_api import MySQLApi


class AlterOperationCategory(Enum):
    COLUMN_CHANGE = 'column_change'
    SAFE_NOOP = 'safe_noop'
    REQUIRES_RESYNC = 'requires_resync'
    UNKNOWN = 'unknown'


@dataclass(frozen=True)
class AlterOperation:
    category: AlterOperationCategory
    action: str
    tokens: tuple
    clause: str


class UnsupportedAlterOperation(Exception):
    pass


CHARSET_MYSQL_TO_PYTHON = {
    'armscii8': None,          # ARMSCII-8 is not directly supported in Python
    'ascii': 'ascii',
    'big5': 'big5',
    'binary': 'latin1',        # Treat binary data as Latin-1 in Python
    'cp1250': 'cp1250',
    'cp1251': 'cp1251',
    'cp1256': 'cp1256',
    'cp1257': 'cp1257',
    'cp850': 'cp850',
    'cp852': 'cp852',
    'cp866': 'cp866',
    'cp932': 'cp932',
    'dec8': 'latin1',          # DEC8 is similar to Latin-1
    'eucjpms': 'euc_jp',       # Map to EUC-JP
    'euckr': 'euc_kr',
    'gb18030': 'gb18030',
    'gb2312': 'gb2312',
    'gbk': 'gbk',
    'geostd8': None,           # GEOSTD8 is not directly supported in Python
    'greek': 'iso8859_7',
    'hebrew': 'iso8859_8',
    'hp8': None,               # HP8 is not directly supported in Python
    'keybcs2': None,           # KEYBCS2 is not directly supported in Python
    'koi8r': 'koi8_r',
    'koi8u': 'koi8_u',
    'latin1': 'cp1252',        # MySQL's latin1 corresponds to Windows-1252
    'latin2': 'iso8859_2',
    'latin5': 'iso8859_9',
    'latin7': 'iso8859_13',
    'macce': 'mac_latin2',
    'macroman': 'mac_roman',
    'sjis': 'shift_jis',
    'swe7': None,              # SWE7 is not directly supported in Python
    'tis620': 'tis_620',
    'ucs2': 'utf_16',          # UCS-2 can be mapped to UTF-16
    'ujis': 'euc_jp',
    'utf16': 'utf_16',
    'utf16le': 'utf_16_le',
    'utf32': 'utf_32',
    'utf8mb3': 'utf_8',        # Both utf8mb3 and utf8mb4 can be mapped to UTF-8
    'utf8mb4': 'utf_8',
    'utf8': 'utf_8',
}


def convert_bytes(obj):
    if isinstance(obj, dict):
        new_obj = {}
        for k, v in obj.items():
            new_key = k.decode('utf-8') if isinstance(k, bytes) else k
            new_value = convert_bytes(v)
            new_obj[new_key] = new_value
        return new_obj
    elif isinstance(obj, (tuple, list)):
        new_obj = []
        for item in obj:
            new_obj.append(convert_bytes(item))
        if isinstance(obj, tuple):
            return tuple(new_obj)
        return new_obj
    elif isinstance(obj, bytes):
        return obj.decode('utf-8')
    else:
        return obj


def parse_mysql_point(binary):
    """
    Parses the binary representation of a MySQL POINT data type
    and returns a tuple (x, y) representing the coordinates.

    :param binary: The binary data representing the POINT.
    :return: A tuple (x, y) with the coordinate values.
    """
    if binary is None:
        return 0, 0

    if len(binary) == 21:
        # No SRID. Proceed as per WKB POINT
        # Read the byte order
        byte_order = binary[0]
        if byte_order == 0:
            endian = '>'
        elif byte_order == 1:
            endian = '<'
        else:
            raise ValueError("Invalid byte order in WKB POINT")
        # Read the WKB Type
        wkb_type = struct.unpack(endian + 'I', binary[1:5])[0]
        if wkb_type != 1:  # WKB type 1 means POINT
            raise ValueError("Not a WKB POINT type")
        # Read X and Y coordinates
        x = struct.unpack(endian + 'd', binary[5:13])[0]
        y = struct.unpack(endian + 'd', binary[13:21])[0]
    elif len(binary) == 25:
        # With SRID included
        # First 4 bytes are the SRID
        srid = struct.unpack('>I', binary[0:4])[0]  # SRID is big-endian
        # Next byte is byte order
        byte_order = binary[4]
        if byte_order == 0:
            endian = '>'
        elif byte_order == 1:
            endian = '<'
        else:
            raise ValueError("Invalid byte order in WKB POINT")
        # Read the WKB Type
        wkb_type = struct.unpack(endian + 'I', binary[5:9])[0]
        if wkb_type != 1:  # WKB type 1 means POINT
            raise ValueError("Not a WKB POINT type")
        # Read X and Y coordinates
        x = struct.unpack(endian + 'd', binary[9:17])[0]
        y = struct.unpack(endian + 'd', binary[17:25])[0]
    else:
        raise ValueError("Invalid binary length for WKB POINT")
    return (x, y)


def parse_mysql_polygon(binary):
    """
    Parses the binary representation of a MySQL POLYGON data type
    and returns a list of tuples [(x1,y1), (x2,y2), ...] representing the polygon vertices.

    :param binary: The binary data representing the POLYGON.
    :return: A list of tuples with the coordinate values.
    """
    if binary is None:
        return []

    # Determine if SRID is present (25 bytes for header with SRID, 21 without)
    has_srid = len(binary) > 25
    offset = 4 if has_srid else 0

    # Read byte order
    byte_order = binary[offset]
    if byte_order == 0:
        endian = '>'
    elif byte_order == 1:
        endian = '<'
    else:
        raise ValueError("Invalid byte order in WKB POLYGON")

    # Read WKB Type
    wkb_type = struct.unpack(endian + 'I', binary[offset + 1:offset + 5])[0]
    if wkb_type != 3:  # WKB type 3 means POLYGON
        raise ValueError("Not a WKB POLYGON type")

    # Read number of rings (polygons can have holes)
    num_rings = struct.unpack(endian + 'I', binary[offset + 5:offset + 9])[0]
    if num_rings == 0:
        return []

    # Read the first ring (outer boundary)
    ring_offset = offset + 9
    num_points = struct.unpack(endian + 'I', binary[ring_offset:ring_offset + 4])[0]
    points = []
    
    # Read each point in the ring
    for i in range(num_points):
        point_offset = ring_offset + 4 + (i * 16)  # 16 bytes per point (8 for x, 8 for y)
        x = struct.unpack(endian + 'd', binary[point_offset:point_offset + 8])[0]
        y = struct.unpack(endian + 'd', binary[point_offset + 8:point_offset + 16])[0]
        points.append((x, y))

    return points


def parse_mysql_multipolygon(binary):
    """
    Parses the binary representation of a MySQL MULTIPOLYGON data type
    and returns a list of polygons, where each polygon is a list of tuples 
    [(x1,y1), (x2,y2), ...] representing the polygon vertices.

    :param binary: The binary data representing the MULTIPOLYGON.
    :return: A list of lists of tuples with the coordinate values.
    """
    if binary is None:
        return []

    # Determine if SRID is present
    has_srid = len(binary) > 25
    offset = 4 if has_srid else 0

    # Read byte order
    byte_order = binary[offset]
    if byte_order == 0:
        endian = '>'
    elif byte_order == 1:
        endian = '<'
    else:
        raise ValueError("Invalid byte order in WKB MULTIPOLYGON")

    # Read WKB Type
    wkb_type = struct.unpack(endian + 'I', binary[offset + 1:offset + 5])[0]
    if wkb_type != 6:  # WKB type 6 means MULTIPOLYGON
        raise ValueError("Not a WKB MULTIPOLYGON type")

    # Read number of polygons
    num_polygons = struct.unpack(endian + 'I', binary[offset + 5:offset + 9])[0]
    if num_polygons == 0:
        return []

    polygons = []
    current_offset = offset + 9

    for polygon_idx in range(num_polygons):
        # Each polygon starts with its own WKB header
        # Read byte order for this polygon
        polygon_byte_order = binary[current_offset]
        if polygon_byte_order == 0:
            polygon_endian = '>'
        elif polygon_byte_order == 1:
            polygon_endian = '<'
        else:
            raise ValueError("Invalid byte order in WKB POLYGON within MULTIPOLYGON")

        # Read WKB Type for this polygon
        polygon_wkb_type = struct.unpack(polygon_endian + 'I', binary[current_offset + 1:current_offset + 5])[0]
        if polygon_wkb_type != 3:  # WKB type 3 means POLYGON
            raise ValueError("Not a WKB POLYGON type within MULTIPOLYGON")

        # Read number of rings for this polygon
        num_rings = struct.unpack(polygon_endian + 'I', binary[current_offset + 5:current_offset + 9])[0]
        if num_rings == 0:
            polygons.append([])
            current_offset += 9
            continue

        # Read the first ring (outer boundary) of this polygon
        ring_offset = current_offset + 9
        num_points = struct.unpack(polygon_endian + 'I', binary[ring_offset:ring_offset + 4])[0]
        points = []

        # Read each point in the ring
        for i in range(num_points):
            point_offset = ring_offset + 4 + (i * 16)  # 16 bytes per point (8 for x, 8 for y)
            x = struct.unpack(polygon_endian + 'd', binary[point_offset:point_offset + 8])[0]
            y = struct.unpack(polygon_endian + 'd', binary[point_offset + 8:point_offset + 16])[0]
            points.append((x, y))

        polygons.append(points)

        # Move to next polygon
        # Skip the current polygon's data: header (9 bytes) + ring header (4 bytes) + points (16 bytes each)
        current_offset = ring_offset + 4 + (num_points * 16)

        # Skip any additional rings (holes) for this polygon
        for ring_idx in range(1, num_rings):
            ring_num_points = struct.unpack(polygon_endian + 'I', binary[current_offset:current_offset + 4])[0]
            current_offset += 4 + (ring_num_points * 16)

    return polygons


def strip_sql_name(name):
    name = name.strip()
    if name.startswith('`'):
        name = name[1:]
    if name.endswith('`'):
        name = name[:-1]
    return name


def split_high_level(data, delimiter):
    """
    Split a string by a delimiter, ignoring delimiters inside parentheses or quotes.
    
    This function performs a context-aware split, respecting nested structures:
    - Delimiters inside parentheses () are ignored
    - Delimiters inside single quotes '' are ignored
    - Handles nested parentheses at any depth
    
    Args:
        data (str): The string to split
        delimiter (str): The character to split on (typically ',' or ';')
    
    Returns:
        list[str]: List of split segments with whitespace stripped
    
    Examples:
        >>> split_high_level("a,b(c,d),e", ",")
        ['a', 'b(c,d)', 'e']
        
        >>> split_high_level("name varchar(100) DEFAULT 'a,b',id int", ",")
        ["name varchar(100) DEFAULT 'a,b'", 'id int']
    """
    if not data:
        return []

    segments = []
    current_segment = []
    paren_depth = 0
    in_quotes = False

    for i, char in enumerate(data):
        # Handle quote toggling (ignore escaped quotes)
        if char == "'" and (i == 0 or data[i - 1] != '\\'):
            in_quotes = not in_quotes
            current_segment.append(char)
            continue

        # Track parentheses depth only outside quotes
        if not in_quotes:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1

        # Split only at top level (outside parentheses and quotes)
        if char == delimiter and paren_depth == 0 and not in_quotes:
            segment_text = ''.join(current_segment).strip()
            if segment_text:  # Only add non-empty segments
                segments.append(segment_text)
            current_segment = []
            continue

        current_segment.append(char)

    # Add final segment if it exists
    final_segment = ''.join(current_segment).strip()
    if final_segment:
        segments.append(final_segment)

    return segments


def strip_sql_comments(sql_statement):
    return sqlparse.format(sql_statement, strip_comments=True).strip()


def convert_timestamp_to_datetime64(input_str, timezone='UTC'):

    # Define the regex pattern
    pattern = r'^timestamp(?:\((\d+)\))?$'

    # Attempt to match the pattern
    match = re.match(pattern, input_str.strip(), re.IGNORECASE)

    if match:
        # If a precision is provided, include it in the replacement
        precision = match.group(1)
        if precision is not None:
            # Only add timezone info if it's not UTC (to preserve original behavior)
            if timezone == 'UTC':
                return f'DateTime64({precision})'
            else:
                return f'DateTime64({precision}, \'{timezone}\')'
        else:
            # Only add timezone info if it's not UTC (to preserve original behavior)
            if timezone == 'UTC':
                return 'DateTime64'
            else:
                return f'DateTime64(3, \'{timezone}\')'
    else:
        raise ValueError(f"Invalid input string format: '{input_str}'")


class MysqlToClickhouseConverter:
    def __init__(self, db_replicator: 'DbReplicator' = None):  # noqa: F821
        self.db_replicator = db_replicator
        self.types_mapping = {}
        if self.db_replicator is not None:
            self.types_mapping = db_replicator.config.types_mapping

    def convert_type(self, mysql_type, parameters):
        is_unsigned = 'unsigned' in parameters.lower()

        result_type = self.types_mapping.get(mysql_type)
        if result_type is not None:
            return result_type

        if mysql_type == 'point':
            return 'Tuple(x Float32, y Float32)'

        if mysql_type == 'polygon':
            return 'Array(Tuple(x Float32, y Float32))'

        if mysql_type == 'multipolygon':
            return 'Array(Array(Tuple(x Float32, y Float32)))'

        # Correctly handle numeric types
        if mysql_type.startswith('numeric'):
            # Determine if parameters are specified via parentheses:
            if '(' in mysql_type and ')' in mysql_type:
                # Expecting a type definition like "numeric(precision, scale)"
                pattern = r"numeric\((\d+)\s*,\s*(\d+)\)"
                match = re.search(pattern, mysql_type)
                if not match:
                    raise ValueError(f"Invalid numeric type definition: {mysql_type}")

                precision = int(match.group(1))
                scale = int(match.group(2))
            else:
                # If no parentheses are provided, assume defaults.
                precision = 10  # or other default as defined by your standards
                scale = 0

            # If no fractional part, consider mapping to integer type (if desired)
            if scale == 0:
                if is_unsigned:
                    if precision <= 9:
                        return "UInt32"
                    elif precision <= 18:
                        return "UInt64"
                    else:
                        # For very large precisions, fallback to Decimal
                        return f"Decimal({precision}, {scale})"
                else:
                    if precision <= 9:
                        return "Int32"
                    elif precision <= 18:
                        return "Int64"
                    else:
                        return f"Decimal({precision}, {scale})"
            else:
                # For types with a defined fractional part, use a Decimal mapping.
                return f"Decimal({precision}, {scale})"

        if mysql_type == 'int':
            if is_unsigned:
                return 'UInt32'
            return 'Int32'
        if mysql_type == 'integer':
            if is_unsigned:
                return 'UInt32'
            return 'Int32'
        if mysql_type == 'bigint':
            if is_unsigned:
                return 'UInt64'
            return 'Int64'
        if mysql_type == 'double':
            return 'Float64'
        if mysql_type == 'real':
            return 'Float64'
        if mysql_type == 'float':
            return 'Float32'
        if mysql_type == 'date':
            return 'Date32'
        if mysql_type == 'tinyint(1)':
            return 'Bool'
        if mysql_type == 'bit(1)':
            return 'Bool'
        if mysql_type in ('bool', 'boolean'):
            return 'Bool'
        if 'smallint' in mysql_type:
            if is_unsigned:
                return 'UInt16'
            return 'Int16'
        if 'tinyint' in mysql_type:
            if is_unsigned:
                return 'UInt8'
            return 'Int8'
        if 'mediumint' in mysql_type:
            if is_unsigned:
                return 'UInt32'
            return 'Int32'
        if 'datetime' in mysql_type:
            return mysql_type.replace('datetime', 'DateTime64')
        if 'longtext' in mysql_type:
            return 'String'
        if 'varchar' in mysql_type:
            return 'String'
        if mysql_type.startswith('enum'):
            enum_values = parse_mysql_enum(mysql_type)
            ch_enum_values = []
            for idx, value_name in enumerate(enum_values):
                ch_enum_values.append(f"'{value_name.lower()}' = {idx+1}")
            ch_enum_values = ', '.join(ch_enum_values)
            if len(enum_values) <= 127:
                # Enum8('red' = 1, 'green' = 2, 'black' = 3)
                return f'Enum8({ch_enum_values})'
            else:
                # Enum16('red' = 1, 'green' = 2, 'black' = 3)
                return f'Enum16({ch_enum_values})'
        if 'text' in mysql_type:
            return 'String'
        if 'blob' in mysql_type:
            return 'String'
        if 'char' in mysql_type:
            return 'String'
        if 'json' in mysql_type:
            return 'String'
        if 'decimal' in mysql_type:
            return 'Float64'
        if 'float' in mysql_type:
            return 'Float32'
        if 'double' in mysql_type:
            return 'Float64'
        if 'bigint' in mysql_type:
            if is_unsigned:
                return 'UInt64'
            return 'Int64'
        if 'integer' in mysql_type or 'int(' in mysql_type:
            if is_unsigned:
                return 'UInt32'
            return 'Int32'
        if 'real' in mysql_type:
            return 'Float64'
        if mysql_type.startswith('timestamp'):
            timezone = 'UTC'
            if self.db_replicator is not None:
                timezone = self.db_replicator.config.mysql_timezone
            return convert_timestamp_to_datetime64(mysql_type, timezone)
        if mysql_type.startswith('time'):
            return 'String'
        if 'varbinary' in mysql_type:
            return 'String'
        if 'binary' in mysql_type:
            return 'String'
        if 'set(' in mysql_type:
            return 'String'
        if mysql_type == 'year':
            return 'UInt16'  # MySQL YEAR type can store years from 1901 to 2155, UInt16 is sufficient
        raise Exception(f'unknown mysql type "{mysql_type}"')

    def convert_field_type(self, mysql_type, mysql_parameters):
        mysql_type = mysql_type.lower()
        mysql_parameters = mysql_parameters.lower()
        not_null = 'not null' in mysql_parameters
        clickhouse_type = self.convert_type(mysql_type, mysql_parameters)
        if 'Tuple' in clickhouse_type:
            not_null = True
        if not not_null:
            clickhouse_type = f'Nullable({clickhouse_type})'
        return clickhouse_type

    def convert_table_structure(self, mysql_structure: TableStructure) -> TableStructure:
        clickhouse_structure = TableStructure()
        clickhouse_structure.table_name = mysql_structure.table_name
        clickhouse_structure.if_not_exists = mysql_structure.if_not_exists
        for field in mysql_structure.fields:
            clickhouse_field_type = self.convert_field_type(field.field_type, field.parameters)
            clickhouse_structure.fields.append(TableField(
                name=field.name,
                field_type=clickhouse_field_type,
            ))
        clickhouse_structure.primary_keys = mysql_structure.primary_keys
        clickhouse_structure.preprocess()
        return clickhouse_structure

    def convert_records(
            self, mysql_records, mysql_structure: TableStructure, clickhouse_structure: TableStructure,
            only_primary: bool = False,
    ):
        mysql_field_types = [field.field_type for field in mysql_structure.fields]
        clickhouse_filed_types = [field.field_type for field in clickhouse_structure.fields]

        clickhouse_records = []
        for mysql_record in mysql_records:
            clickhouse_record = self.convert_record(
                mysql_record, mysql_field_types, clickhouse_filed_types, mysql_structure, only_primary,
            )
            clickhouse_records.append(clickhouse_record)
        return clickhouse_records

    def convert_record(
            self, mysql_record, mysql_field_types, clickhouse_field_types, mysql_structure: TableStructure,
            only_primary: bool,
    ):
        clickhouse_record = []
        for idx, mysql_field_value in enumerate(mysql_record):
            if only_primary and idx not in mysql_structure.primary_key_ids:
                clickhouse_record.append(mysql_field_value)
                continue

            clickhouse_field_value = mysql_field_value
            mysql_field_type = mysql_field_types[idx]
            clickhouse_field_type = clickhouse_field_types[idx]
            if mysql_field_type.startswith('time') and 'String' in clickhouse_field_type:
                clickhouse_field_value = str(mysql_field_value)
            if mysql_field_type == 'json' and 'String' in clickhouse_field_type:
                if not isinstance(clickhouse_field_value, str):
                    clickhouse_field_value = json.dumps(convert_bytes(clickhouse_field_value))

            if mysql_field_type.startswith('point'):
                clickhouse_field_value = parse_mysql_point(clickhouse_field_value)

            if mysql_field_type.startswith('polygon'):
                clickhouse_field_value = parse_mysql_polygon(clickhouse_field_value)

            if mysql_field_type.startswith('multipolygon'):
                clickhouse_field_value = parse_mysql_multipolygon(clickhouse_field_value)

            if mysql_field_type.startswith('enum('):
                enum_values = mysql_structure.fields[idx].additional_data
                field_name = mysql_structure.fields[idx].name if idx < len(mysql_structure.fields) else "unknown"
                
                clickhouse_field_value = EnumConverter.convert_mysql_to_clickhouse_enum(
                    clickhouse_field_value,
                    enum_values,
                    field_name
                )

            # Handle MySQL YEAR type conversion
            if mysql_field_type == 'year' and clickhouse_field_value is not None:
                # MySQL YEAR type can store years from 1901 to 2155
                # Convert to integer if it's a string
                if isinstance(clickhouse_field_value, str):
                    clickhouse_field_value = int(clickhouse_field_value)
                # Ensure the value is within valid range
                if clickhouse_field_value < 1901:
                    clickhouse_field_value = 1901
                elif clickhouse_field_value > 2155:
                    clickhouse_field_value = 2155

            if clickhouse_field_value is not None:
                if 'UUID' in clickhouse_field_type:
                    if len(clickhouse_field_value) == 36:
                        if isinstance(clickhouse_field_value, bytes):
                            clickhouse_field_value = clickhouse_field_value.decode('utf-8')
                        clickhouse_field_value = uuid.UUID(clickhouse_field_value).bytes

                if 'UInt16' in clickhouse_field_type and clickhouse_field_value < 0:
                    clickhouse_field_value = 65536 + clickhouse_field_value
                if 'UInt8' in clickhouse_field_type and clickhouse_field_value < 0:
                    clickhouse_field_value = 256 + clickhouse_field_value
                if 'mediumint' in mysql_field_type.lower() and clickhouse_field_value < 0:
                    clickhouse_field_value = 16777216 + clickhouse_field_value
                if 'UInt32' in clickhouse_field_type and clickhouse_field_value < 0:
                    clickhouse_field_value = 4294967296 + clickhouse_field_value
                if 'UInt64' in clickhouse_field_type and clickhouse_field_value < 0:
                    clickhouse_field_value = 18446744073709551616 + clickhouse_field_value

                if 'String' in clickhouse_field_type and (
                        'text' in mysql_field_type or 'char' in mysql_field_type
                ):
                    if isinstance(clickhouse_field_value, bytes):
                        charset = mysql_structure.charset_python or 'utf-8'
                        clickhouse_field_value = clickhouse_field_value.decode(charset)

                if 'set(' in mysql_field_type:
                    set_values = mysql_structure.fields[idx].additional_data
                    if isinstance(clickhouse_field_value, int):
                        bit_mask = clickhouse_field_value
                        clickhouse_field_value = [
                            val
                            for idx, val in enumerate(set_values)
                            if bit_mask & (1 << idx)
                        ]
                    elif isinstance(clickhouse_field_value, set):
                        clickhouse_field_value = [
                            v for v in set_values if v in clickhouse_field_value
                        ]
                    clickhouse_field_value = ','.join(clickhouse_field_value)
            else:
                # Handle NULL values for non-nullable ClickHouse columns
                # Convert NULL to appropriate default value based on ClickHouse type
                if 'Nullable' not in clickhouse_field_type:
                    clickhouse_field_value = self.__get_default_value_for_type_python(clickhouse_field_type)

            clickhouse_record.append(clickhouse_field_value)
        return tuple(clickhouse_record)

    def __basic_validate_query(self, mysql_query):
        mysql_query = mysql_query.strip()
        if mysql_query.endswith(';'):
            mysql_query = mysql_query[:-1]
        if mysql_query.find(';') != -1:
            raise Exception('multi-query statement not supported')
        return mysql_query
    
    def get_db_and_table_name(self, token, db_name):
        if '.' in token:
            db_name, table_name = token.split('.')
        else:
            table_name = token
        db_name = strip_sql_name(db_name)
        table_name = strip_sql_name(table_name)

        if self.db_replicator:
            # If we're dealing with a relative table name (no DB prefix), we need to check
            # if the current db_name is already a target database name
            if '.' not in token and self.db_replicator.target_database == db_name:
                # This is a target database name, so for config matching we need to use the source database
                matches_config = (
                    self.db_replicator.config.is_database_matches(self.db_replicator.database)
                    and self.db_replicator.config.is_table_matches(table_name))
            else:
                # Normal case: check if source database and table match config
                matches_config = (
                    self.db_replicator.config.is_database_matches(db_name)
                    and self.db_replicator.config.is_table_matches(table_name))
            
            # Apply database mapping AFTER checking matches_config
            if db_name == self.db_replicator.database:
                db_name = self.db_replicator.target_database
        else:
            matches_config = True

        return db_name, table_name, matches_config

    def parse_alter_query(self, mysql_query, db_name):
        mysql_query = self.__basic_validate_query(mysql_query)

        tokens = mysql_query.split()
        if len(tokens) < 4 or tokens[0].lower() != 'alter':
            raise Exception('wrong query')

        if tokens[1].lower() != 'table':
            raise Exception('wrong query')

        db_name, table_name, matches_config = self.get_db_and_table_name(tokens[2], db_name)

        if not matches_config:
            return db_name, table_name, matches_config, []

        subqueries = ' '.join(tokens[3:])
        subqueries = split_high_level(subqueries, ',')
        operations = []

        for subquery in subqueries:
            subquery = subquery.strip()
            tokens = subquery.split()
            if not tokens:
                raise Exception(f'empty alter operation, query: {mysql_query}')

            op_name = tokens[0].lower()
            tokens = tokens[1:]

            operations.append(self._classify_alter_operation(op_name, tokens, subquery))

        return db_name, table_name, matches_config, operations

    @staticmethod
    def _starts_with_keyword(token, keywords):
        token = token.lower()
        return token in keywords or any(token.startswith(f'{keyword}(') for keyword in keywords)

    def _classify_alter_operation(self, op_name, tokens, subquery):
        tokens = list(tokens)
        if '=' in op_name:
            op_name, inline_value = op_name.split('=', 1)
            tokens.insert(0, inline_value)

        if op_name in ('add', 'drop', 'modify', 'change') and tokens and tokens[0].lower() == 'column':
            tokens = tokens[1:]

        if op_name == 'add':
            if not tokens:
                return AlterOperation(AlterOperationCategory.UNKNOWN, op_name, tuple(tokens), subquery)
            first_token = tokens[0].lower()
            if first_token == 'partition' or first_token.startswith('partition('):
                return AlterOperation(AlterOperationCategory.REQUIRES_RESYNC, op_name, tuple(tokens), subquery)
            if first_token == 'primary':
                return AlterOperation(AlterOperationCategory.SAFE_NOOP, op_name, tuple(tokens), subquery)
            ignored_keywords = ('constraint', 'index', 'foreign', 'unique', 'key', 'fulltext', 'spatial', 'check')
            if self._starts_with_keyword(tokens[0], ignored_keywords):
                return AlterOperation(AlterOperationCategory.SAFE_NOOP, op_name, tuple(tokens), subquery)
            if first_token.startswith('('):
                return AlterOperation(AlterOperationCategory.REQUIRES_RESYNC, op_name, tuple(tokens), subquery)
            return AlterOperation(AlterOperationCategory.COLUMN_CHANGE, 'add_column', tuple(tokens), subquery)

        if op_name == 'drop':
            if not tokens:
                return AlterOperation(AlterOperationCategory.UNKNOWN, op_name, tuple(tokens), subquery)
            first_token = tokens[0].lower()
            if first_token == 'partition' or first_token.startswith('partition('):
                return AlterOperation(AlterOperationCategory.REQUIRES_RESYNC, op_name, tuple(tokens), subquery)
            if first_token == 'primary':
                return AlterOperation(AlterOperationCategory.SAFE_NOOP, op_name, tuple(tokens), subquery)
            ignored_keywords = ('constraint', 'check', 'index', 'foreign', 'unique', 'key')
            if self._starts_with_keyword(tokens[0], ignored_keywords):
                return AlterOperation(AlterOperationCategory.SAFE_NOOP, op_name, tuple(tokens), subquery)
            if len(tokens) == 1:
                return AlterOperation(AlterOperationCategory.COLUMN_CHANGE, 'drop_column', tuple(tokens), subquery)
            return AlterOperation(AlterOperationCategory.UNKNOWN, op_name, tuple(tokens), subquery)

        if op_name == 'modify':
            if not tokens:
                return AlterOperation(AlterOperationCategory.UNKNOWN, op_name, tuple(tokens), subquery)
            return AlterOperation(AlterOperationCategory.COLUMN_CHANGE, 'modify_column', tuple(tokens), subquery)

        if op_name == 'change':
            if not tokens:
                return AlterOperation(AlterOperationCategory.UNKNOWN, op_name, tuple(tokens), subquery)
            return AlterOperation(AlterOperationCategory.COLUMN_CHANGE, 'change_column', tuple(tokens), subquery)

        if op_name == 'rename':
            if not tokens:
                return AlterOperation(AlterOperationCategory.UNKNOWN, op_name, tuple(tokens), subquery)
            first_token = tokens[0].lower()
            if first_token in ('index', 'key'):
                return AlterOperation(AlterOperationCategory.SAFE_NOOP, op_name, tuple(tokens), subquery)
            if first_token == 'column':
                return AlterOperation(AlterOperationCategory.COLUMN_CHANGE, 'rename_column', tuple(tokens[1:]), subquery)
            return AlterOperation(AlterOperationCategory.REQUIRES_RESYNC, op_name, tuple(tokens), subquery)

        safe_noop_operations = {
            'algorithm', 'alter', 'auto_increment', 'avg_row_length', 'character',
            'checksum', 'collate', 'comment', 'compression', 'connection', 'convert',
            'data', 'default', 'disable', 'discard', 'enable', 'encryption', 'engine',
            'engine_attribute', 'force', 'import', 'index', 'insert_method',
            'key_block_size', 'lock', 'max_rows', 'min_rows', 'order', 'pack_keys',
            'password', 'row_format', 'secondary_engine_attribute', 'stats_auto_recalc',
            'stats_persistent', 'stats_sample_pages', 'tablespace', 'union', 'with',
            'without',
        }
        if op_name in safe_noop_operations:
            return AlterOperation(AlterOperationCategory.SAFE_NOOP, op_name, tuple(tokens), subquery)

        partition_operations = {
            'analyze', 'check', 'coalesce', 'exchange', 'optimize', 'rebuild',
            'remove', 'reorganize', 'repair', 'truncate',
        }
        if op_name in partition_operations:
            return AlterOperation(AlterOperationCategory.REQUIRES_RESYNC, op_name, tuple(tokens), subquery)

        return AlterOperation(AlterOperationCategory.UNKNOWN, op_name, tuple(tokens), subquery)

    def convert_alter_query(self, mysql_query, db_name):
        db_name, table_name, matches_config, operations = self.parse_alter_query(mysql_query, db_name)

        if not matches_config:
            return

        unsupported_operations = [
            operation for operation in operations
            if operation.category in (AlterOperationCategory.REQUIRES_RESYNC, AlterOperationCategory.UNKNOWN)
        ]
        if unsupported_operations:
            operation = unsupported_operations[0]
            raise UnsupportedAlterOperation(
                f'unsupported ALTER TABLE operation ({operation.category.value}): {operation.clause}; '
                f'full query: {mysql_query}'
            )

        self._validate_alter_plan(table_name, operations)

        for operation in operations:
            if operation.category == AlterOperationCategory.SAFE_NOOP:
                continue

            tokens = list(operation.tokens)
            if operation.action == 'add_column':
                self.__convert_alter_table_add_column(db_name, table_name, tokens)
            elif operation.action == 'drop_column':
                self.__convert_alter_table_drop_column(db_name, table_name, tokens)
            elif operation.action == 'modify_column':
                self.__convert_alter_table_modify_column(db_name, table_name, tokens)
            elif operation.action == 'change_column':
                self.__convert_alter_table_change_column(db_name, table_name, tokens)
            elif operation.action == 'rename_column':
                self.__convert_alter_table_rename_column(db_name, table_name, tokens)

    def _validate_alter_plan(self, table_name, operations):
        mysql_structure = None
        ch_structure = None
        if self.db_replicator:
            if table_name not in self.db_replicator.state.tables_structure:
                raise Exception(f'table {table_name} not found in replicator state')
            mysql_structure, ch_structure = copy.deepcopy(
                self.db_replicator.state.tables_structure[table_name]
            )

        for operation in operations:
            if operation.category == AlterOperationCategory.SAFE_NOOP:
                continue
            self._validate_alter_operation(operation, mysql_structure, ch_structure)

    def _validate_alter_operation(self, operation, mysql_structure, ch_structure):
        tokens = list(operation.tokens)

        if operation.action == 'add_column':
            column_name, column_type_mysql, _, column_after, column_first, column_type_ch = self._parse_add_column(tokens)
            if mysql_structure is None:
                return
            mysql_exists = mysql_structure.has_field(column_name)
            ch_exists = ch_structure.has_field(column_name)
            if mysql_exists != ch_exists:
                raise Exception(f'inconsistent table structures for column {column_name}')
            if mysql_exists:
                return
            if column_first:
                mysql_structure.add_field_first(TableField(name=column_name, field_type=column_type_mysql))
                ch_structure.add_field_first(TableField(name=column_name, field_type=column_type_ch))
            else:
                if column_after is None:
                    column_after = mysql_structure.fields[-1].name
                mysql_structure.add_field_after(TableField(name=column_name, field_type=column_type_mysql), column_after)
                ch_structure.add_field_after(TableField(name=column_name, field_type=column_type_ch), column_after)
            return

        if operation.action == 'drop_column':
            column_name = self._parse_drop_column(tokens)
            if mysql_structure is None:
                return
            mysql_exists = mysql_structure.has_field(column_name)
            ch_exists = ch_structure.has_field(column_name)
            if mysql_exists != ch_exists:
                raise Exception(f'inconsistent table structures for column {column_name}')
            if not mysql_exists:
                return
            mysql_structure.remove_field(column_name)
            ch_structure.remove_field(column_name)
            return

        if operation.action == 'modify_column':
            column_name, column_type_mysql, _, column_type_ch = self._parse_modify_column(tokens)
            if mysql_structure is None:
                return
            if not mysql_structure.has_field(column_name) or not ch_structure.has_field(column_name):
                raise Exception(f'field {column_name} not found')
            mysql_structure.update_field(TableField(name=column_name, field_type=column_type_mysql))
            ch_structure.update_field(TableField(name=column_name, field_type=column_type_ch))
            return

        if operation.action == 'change_column':
            old_name, new_name, column_type_mysql, _, column_type_ch = self._parse_change_column(tokens)
            if mysql_structure is None:
                return
            current_name = self._get_change_column_current_name(mysql_structure, ch_structure, old_name, new_name)
            mysql_structure.update_field(TableField(name=current_name, field_type=column_type_mysql))
            ch_structure.update_field(TableField(name=current_name, field_type=column_type_ch))
            if current_name != new_name:
                self._rename_structure_field(mysql_structure, current_name, new_name)
                self._rename_structure_field(ch_structure, current_name, new_name)
            return

        if operation.action == 'rename_column':
            old_name, new_name = self._parse_rename_column(tokens)
            if mysql_structure is None:
                return
            current_name = self._get_change_column_current_name(mysql_structure, ch_structure, old_name, new_name)
            if current_name != new_name:
                self._rename_structure_field(mysql_structure, current_name, new_name)
                self._rename_structure_field(ch_structure, current_name, new_name)
            return

    @classmethod
    def _tokenize_alter_query(cls, sql_line):
        # We want to recognize tokens that may be:
        # 1. A backquoted identifier that can optionally be immediately followed by parentheses.
        # 2. A plain word (letters/digits/underscore) that may immediately be followed by a parenthesized argument list.
        # 3. A single-quoted or double-quoted string.
        # 4. Or, if nothing else, any non‐whitespace sequence.
        #
        # The order is important: for example, if a word is immediately followed by parentheses,
        # we want to grab it as a single token.
        token_pattern = re.compile(r'''
             (                           # start capture group for a token 
               `[^`]+`(?:\([^)]*\))?      |   # backquoted identifier w/ optional parentheses
               \w+(?:\([^)]*\))?          |   # a word with optional parentheses
               '(?:\\'|[^'])*'           |   # a single-quoted string
               "(?:\\"|[^"])*"           |   # a double-quoted string
               [^\s]+                      # fallback: any sequence of non-whitespace characters
             )
             ''', re.VERBOSE)
        tokens = token_pattern.findall(sql_line)

        # Now, split the column definition into:
        #   token0 = column name,
        #   token1 = data type (which might be multiple tokens, e.g. DOUBLE PRECISION, INT UNSIGNED,
        #            or a word+parentheses like VARCHAR(254) or NUMERIC(5, 2)),
        #   remaining tokens: the parameters such as DEFAULT, NOT, etc.
        #
        # We define a set of keywords that indicate the start of column options.
        constraint_keywords = {
            "DEFAULT", "NOT", "NULL", "AUTO_INCREMENT", "PRIMARY", "UNIQUE",
            "COMMENT", "COLLATE", "REFERENCES", "ON", "CHECK", "CONSTRAINT",
            "AFTER", "BEFORE", "GENERATED", "VIRTUAL", "STORED", "FIRST",
            "ALWAYS", "AS", "IDENTITY", "INVISIBLE", "PERSISTED",
        }

        if not tokens:
            return tokens
        # The first token is always the column name.
        column_name = tokens[0]

        # Now "merge" tokens after the column name that belong to the type.
        # (For many types the type is written as a single token already –
        #  e.g. "VARCHAR(254)" or "NUMERIC(5, 2)", but for types like
        #  "DOUBLE PRECISION" or "INT UNSIGNED" the .split() would produce two tokens.)
        type_tokens = []
        i = 1
        while i < len(tokens) and tokens[i].upper() not in constraint_keywords:
            type_tokens.append(tokens[i])
            i += 1
        merged_type = " ".join(type_tokens) if type_tokens else ""

        # The remaining tokens are passed through unchanged.
        param_tokens = tokens[i:]

        # Result: [column name, merged type, all the rest]
        if merged_type:
            return [column_name, merged_type] + param_tokens
        else:
            return [column_name] + param_tokens

    def _parse_add_column(self, tokens):
        tokens = self._tokenize_alter_query(' '.join(tokens))
        if len(tokens) < 2:
            raise Exception('wrong tokens count', tokens)

        column_after = None
        column_first = False
        if len(tokens) >= 2 and tokens[-2].lower() == 'after':
            column_after = strip_sql_name(tokens[-1])
            tokens = tokens[:-2]
        elif tokens[-1].lower() == 'first':
            column_first = True
            tokens = tokens[:-1]

        if len(tokens) < 2:
            raise Exception('wrong tokens count', tokens)

        column_name = strip_sql_name(tokens[0])
        column_type_mysql = tokens[1]
        column_type_mysql_parameters = ' '.join(tokens[2:])
        column_type_ch = self.convert_field_type(column_type_mysql, column_type_mysql_parameters)
        return (
            column_name, column_type_mysql, column_type_mysql_parameters,
            column_after, column_first, column_type_ch,
        )

    @staticmethod
    def _parse_drop_column(tokens):
        if len(tokens) != 1:
            raise Exception('wrong tokens count', tokens)
        return strip_sql_name(tokens[0])

    def _parse_modify_column(self, tokens):
        tokens = self._tokenize_alter_query(' '.join(tokens))
        if len(tokens) < 2:
            raise Exception('wrong tokens count', tokens)
        column_name = strip_sql_name(tokens[0])
        column_type_mysql = tokens[1]
        column_type_mysql_parameters = ' '.join(tokens[2:])
        column_type_ch = self.convert_field_type(column_type_mysql, column_type_mysql_parameters)
        return column_name, column_type_mysql, column_type_mysql_parameters, column_type_ch

    def _parse_change_column(self, tokens):
        if len(tokens) < 3:
            raise Exception('wrong tokens count', tokens)
        old_column_name = strip_sql_name(tokens[0])
        new_column_name = strip_sql_name(tokens[1])
        column_tokens = self._tokenize_alter_query(' '.join([tokens[1]] + tokens[2:]))
        if len(column_tokens) < 2:
            raise Exception('wrong tokens count', tokens)
        column_type_mysql = column_tokens[1]
        column_type_mysql_parameters = ' '.join(column_tokens[2:])
        column_type_ch = self.convert_field_type(column_type_mysql, column_type_mysql_parameters)
        return (
            old_column_name, new_column_name, column_type_mysql,
            column_type_mysql_parameters, column_type_ch,
        )

    @staticmethod
    def _parse_rename_column(tokens):
        if len(tokens) != 3:
            raise Exception('wrong tokens count for RENAME COLUMN', tokens)
        if tokens[1].lower() != 'to':
            raise Exception('expected TO keyword in RENAME COLUMN syntax', tokens)
        return strip_sql_name(tokens[0]), strip_sql_name(tokens[2])

    @staticmethod
    def _rename_structure_field(structure, old_name, new_name):
        field = structure.get_field(old_name)
        if field is None:
            raise Exception(f'Column {old_name} not found in structure')
        field.name = new_name
        structure.primary_keys = [new_name if key == old_name else key for key in structure.primary_keys]
        structure.preprocess()

    @staticmethod
    def _get_change_column_current_name(mysql_structure, ch_structure, old_name, new_name):
        mysql_old = mysql_structure.has_field(old_name)
        ch_old = ch_structure.has_field(old_name)
        mysql_new = mysql_structure.has_field(new_name)
        ch_new = ch_structure.has_field(new_name)

        if mysql_old and ch_old and (old_name == new_name or not mysql_new and not ch_new):
            return old_name
        if old_name != new_name and mysql_new and ch_new and not mysql_old and not ch_old:
            return new_name
        raise Exception(f'inconsistent table structures for column rename {old_name} to {new_name}')

    def __convert_alter_table_add_column(self, db_name, table_name, tokens):
        (
            column_name, column_type_mysql, _, column_after,
            column_first, column_type_ch,
        ) = self._parse_add_column(tokens)

        mysql_table_structure = None
        ch_table_structure = None
        if self.db_replicator:
            table_structure = self.db_replicator.state.tables_structure[table_name]
            mysql_table_structure, ch_table_structure = table_structure
            if mysql_table_structure.has_field(column_name) and ch_table_structure.has_field(column_name):
                return
            if column_after is None and not column_first:
                column_after = strip_sql_name(mysql_table_structure.fields[-1].name)

        target_table_name = self.db_replicator.get_target_table_name(table_name) if self.db_replicator else table_name
        on_cluster = self.db_replicator.clickhouse_api.get_on_cluster_clause() if self.db_replicator else ''
        query = f'ALTER TABLE `{db_name}`.`{target_table_name}` {on_cluster} ADD COLUMN IF NOT EXISTS `{column_name}` {column_type_ch}'
        if column_first:
            query += ' FIRST'
        elif column_after is not None:
            query += f' AFTER {column_after}'

        if self.db_replicator:
            self.db_replicator.clickhouse_api.execute_command(query)
            if column_first:
                mysql_table_structure.add_field_first(TableField(name=column_name, field_type=column_type_mysql))
                ch_table_structure.add_field_first(TableField(name=column_name, field_type=column_type_ch))
            else:
                mysql_table_structure.add_field_after(
                    TableField(name=column_name, field_type=column_type_mysql), column_after,
                )
                ch_table_structure.add_field_after(
                    TableField(name=column_name, field_type=column_type_ch), column_after,
                )

    def __convert_alter_table_drop_column(self, db_name, table_name, tokens):
        column_name = self._parse_drop_column(tokens)

        mysql_table_structure = None
        ch_table_structure = None
        if self.db_replicator:
            table_structure = self.db_replicator.state.tables_structure[table_name]
            mysql_table_structure, ch_table_structure = table_structure
            if not mysql_table_structure.has_field(column_name) and not ch_table_structure.has_field(column_name):
                return

        target_table_name = self.db_replicator.get_target_table_name(table_name) if self.db_replicator else table_name
        on_cluster = self.db_replicator.clickhouse_api.get_on_cluster_clause() if self.db_replicator else ''
        query = f'ALTER TABLE `{db_name}`.`{target_table_name}` {on_cluster} DROP COLUMN IF EXISTS `{column_name}`'
        if self.db_replicator:
            self.db_replicator.clickhouse_api.execute_command(query)
            mysql_table_structure.remove_field(field_name=column_name)
            ch_table_structure.remove_field(field_name=column_name)

    def __convert_alter_table_modify_column(self, db_name, table_name, tokens):
        column_name, column_type_mysql, column_type_mysql_parameters, column_type_ch = self._parse_modify_column(tokens)

        mysql_table_structure = None
        ch_table_structure = None
        if self.db_replicator:
            table_structure = self.db_replicator.state.tables_structure[table_name]
            mysql_table_structure, ch_table_structure = table_structure

        target_table_name = self.db_replicator.get_target_table_name(table_name) if self.db_replicator else table_name
        on_cluster = self.db_replicator.clickhouse_api.get_on_cluster_clause() if self.db_replicator else ''
        # Check if we're converting from nullable to non-nullable
        default_clause = ''
        if self.db_replicator and 'not null' in column_type_mysql_parameters.lower():
            # When converting to NOT NULL in MySQL, we need to add DEFAULT in ClickHouse
            # because ClickHouse requires DEFAULT when converting from nullable to non-nullable
            current_field = ch_table_structure.get_field(column_name)
            if current_field:
                # Always add DEFAULT when converting to NOT NULL in MySQL
                # because ClickHouse might have the column as nullable even if we think it's not
                # Extract the base type (remove Nullable wrapper if present)
                field_type = current_field.field_type
                if field_type.startswith('Nullable('):
                    inner_type = field_type[9:-1]
                else:
                    inner_type = field_type
                default_clause = f' DEFAULT {self.__get_default_value_for_type(inner_type)}'
        
        query = f'ALTER TABLE `{db_name}`.`{target_table_name}` {on_cluster} MODIFY COLUMN IF EXISTS `{column_name}` {column_type_ch}{default_clause}'
        if self.db_replicator:
            self.db_replicator.clickhouse_api.execute_command(query)
            mysql_table_structure.update_field(TableField(name=column_name, field_type=column_type_mysql))
            ch_table_structure.update_field(TableField(name=column_name, field_type=column_type_ch))

    def __get_default_value_for_type(self, ch_type: str) -> str:
        """Get appropriate default value for ClickHouse type when converting from nullable to non-nullable"""
        ch_type_lower = ch_type.lower().strip()
        
        # Handle numeric types
        if ch_type_lower in ['int8', 'int16', 'int32', 'int64', 'int128', 'int256']:
            return '0'
        if ch_type_lower in ['uint8', 'uint16', 'uint32', 'uint64', 'uint128', 'uint256']:
            return '0'
        if ch_type_lower in ['float32', 'float64']:
            return '0.0'
        if ch_type_lower == 'decimal':
            return '0'
            
        # Handle string types
        if ch_type_lower in ['string', 'fixedstring']:
            return "''"
            
        # Handle date/time types
        if ch_type_lower == 'date':
            return "'1970-01-01'"
        if ch_type_lower.startswith('datetime'):
            return "'1970-01-01 00:00:00'"
        if ch_type_lower.startswith('date32'):
            return "'1970-01-01'"
            
        # Handle UUID
        if ch_type_lower == 'uuid':
            return "'00000000-0000-0000-0000-000000000000'"
            
        # Handle IP addresses
        if ch_type_lower == 'ipv4':
            return "'0.0.0.0'"
        if ch_type_lower == 'ipv6':
            return "'::'"
            
        # Handle boolean
        if ch_type_lower == 'bool':
            return 'false'
            
        # For complex types like Array, Tuple, etc., use empty/default values
        if ch_type_lower.startswith('array'):
            return '[]'
        if ch_type_lower.startswith('tuple'):
            return '()'
        if ch_type_lower.startswith('map'):
            return '{}'
            
        # For enum types, try to get the first value
        if ch_type_lower.startswith('enum'):
            # Extract first enum value - format is Enum8('value1'=1, 'value2'=2) or similar
            match = re.search(r"Enum\d+\('([^']+)'", ch_type)
            if match:
                return f"'{match.group(1)}'"
            return "''"
            
        # Default fallback
        return "''"

    def __get_default_value_for_type_python(self, ch_type: str):
        """Get appropriate default value as Python native type for ClickHouse type"""
        ch_type_lower = ch_type.lower().strip()
        
        # Handle numeric types - return actual Python int/float
        if ch_type_lower in ['int8', 'int16', 'int32', 'int64', 'int128', 'int256']:
            return 0
        if ch_type_lower in ['uint8', 'uint16', 'uint32', 'uint64', 'uint128', 'uint256']:
            return 0
        if ch_type_lower in ['float32', 'float64']:
            return 0.0
        if ch_type_lower == 'decimal':
            return 0
            
        # Handle string types
        if ch_type_lower in ['string', 'fixedstring']:
            return ''
            
        # Handle date/time types - return datetime objects
        if ch_type_lower == 'date':
            return datetime.date(1970, 1, 1)
        if ch_type_lower.startswith('datetime'):
            return datetime.datetime(1970, 1, 1, 0, 0, 0)
        if ch_type_lower.startswith('date32'):
            return datetime.date(1970, 1, 1)
            
        # Handle UUID
        if ch_type_lower == 'uuid':
            return '00000000-0000-0000-0000-000000000000'
            
        # Handle IP addresses
        if ch_type_lower == 'ipv4':
            return '0.0.0.0'
        if ch_type_lower == 'ipv6':
            return '::'
            
        # Handle boolean
        if ch_type_lower == 'bool':
            return False
            
        # For complex types like Array, Tuple, etc., use empty/default values
        if ch_type_lower.startswith('array'):
            return []
        if ch_type_lower.startswith('tuple'):
            return ()
        if ch_type_lower.startswith('map'):
            return {}
            
        # For enum types, try to get first value
        if ch_type_lower.startswith('enum'):
            # Extract first enum value - format is Enum8('value1'=1, 'value2'=2) or similar
            match = re.search(r"Enum\d+\('([^']+)'", ch_type)
            if match:
                return match.group(1)
            return ''
            
        # Default fallback
        return ''

    def __convert_alter_table_change_column(self, db_name, table_name, tokens):
        (
            column_name, new_column_name, column_type_mysql,
            column_type_mysql_parameters, column_type_ch,
        ) = self._parse_change_column(tokens)

        if self.db_replicator:
            table_structure = self.db_replicator.state.tables_structure[table_name]
            mysql_table_structure: TableStructure = table_structure[0]
            ch_table_structure: TableStructure = table_structure[1]
            current_name = self._get_change_column_current_name(
                mysql_table_structure, ch_table_structure, column_name, new_column_name,
            )

            current_column_type_ch = ch_table_structure.get_field(current_name).field_type
            target_table_name = self.db_replicator.get_target_table_name(table_name)
            on_cluster = self.db_replicator.clickhouse_api.get_on_cluster_clause()

            if current_column_type_ch != column_type_ch:
                query = f'ALTER TABLE `{db_name}`.`{target_table_name}` {on_cluster} MODIFY COLUMN IF EXISTS `{current_name}` {column_type_ch}'
                self.db_replicator.clickhouse_api.execute_command(query)
                mysql_table_structure.update_field(
                    TableField(name=current_name, field_type=column_type_mysql),
                )
                ch_table_structure.update_field(
                    TableField(name=current_name, field_type=column_type_ch),
                )

            if current_name != new_column_name:
                query = f'ALTER TABLE `{db_name}`.`{target_table_name}` {on_cluster} RENAME COLUMN IF EXISTS `{current_name}` TO `{new_column_name}`'
                self.db_replicator.clickhouse_api.execute_command(query)
                self._rename_structure_field(mysql_table_structure, current_name, new_column_name)
                self._rename_structure_field(ch_table_structure, current_name, new_column_name)

    def __convert_alter_table_rename_column(self, db_name, table_name, tokens):
        """
        Handle the RENAME COLUMN syntax of ALTER TABLE statements.
        Example: RENAME COLUMN old_name TO new_name
        """
        old_column_name, new_column_name = self._parse_rename_column(tokens)
        current_name = old_column_name
        
        if self.db_replicator:
            if table_name in self.db_replicator.state.tables_structure:
                table_structure = self.db_replicator.state.tables_structure[table_name]
                mysql_table_structure: TableStructure = table_structure[0]
                ch_table_structure: TableStructure = table_structure[1]
                current_name = self._get_change_column_current_name(
                    mysql_table_structure, ch_table_structure, old_column_name, new_column_name,
                )
                if current_name == new_column_name:
                    return

        target_table_name = self.db_replicator.get_target_table_name(table_name) if self.db_replicator else table_name
        on_cluster = self.db_replicator.clickhouse_api.get_on_cluster_clause() if self.db_replicator else ''
        query = f'ALTER TABLE `{db_name}`.`{target_table_name}` {on_cluster} RENAME COLUMN IF EXISTS `{current_name}` TO `{new_column_name}`'
        if self.db_replicator:
            self.db_replicator.clickhouse_api.execute_command(query)
            self._rename_structure_field(mysql_table_structure, current_name, new_column_name)
            self._rename_structure_field(ch_table_structure, current_name, new_column_name)

    def _handle_create_table_like(self, create_statement, source_table_name, target_table_name, is_query_api=True):
        """
        Helper method to handle CREATE TABLE LIKE statements.
        
        Args:
            create_statement: The original CREATE TABLE LIKE statement
            source_table_name: Name of the source table being copied
            target_table_name: Name of the new table being created
            is_query_api: If True, returns both MySQL and CH structures; if False, returns only MySQL structure
            
        Returns:
            Either (mysql_structure, ch_structure) if is_query_api=True, or just mysql_structure otherwise
        """
        # Try to get the actual structure from the existing table structures first
        if (hasattr(self, 'db_replicator') and 
            self.db_replicator is not None and 
            hasattr(self.db_replicator, 'state') and
            hasattr(self.db_replicator.state, 'tables_structure')):
            
            # Check if the source table structure is already in our state
            if source_table_name in self.db_replicator.state.tables_structure:
                # Get the existing structure
                source_mysql_structure, source_ch_structure = self.db_replicator.state.tables_structure[source_table_name]
                
                # Create a new structure with the target table name
                new_mysql_structure = copy.deepcopy(source_mysql_structure)
                new_mysql_structure.table_name = target_table_name
                
                # Convert to ClickHouse structure 
                new_ch_structure = copy.deepcopy(source_ch_structure)
                new_ch_structure.table_name = target_table_name
                
                return (new_mysql_structure, new_ch_structure) if is_query_api else new_mysql_structure
        
        mysql_api = None
        mysql_api_was_provided = False
        
        if (hasattr(self, 'db_replicator') and 
            self.db_replicator is not None and 
            hasattr(self.db_replicator, 'mysql_api') and 
            self.db_replicator.mysql_api is not None):
            mysql_api = self.db_replicator.mysql_api
            mysql_api_was_provided = True
        elif (hasattr(self, 'db_replicator') and 
              self.db_replicator is not None and 
              hasattr(self.db_replicator, 'config') and 
              hasattr(self.db_replicator, 'database')):
            mysql_api = MySQLApi(
                database=self.db_replicator.database,
                mysql_settings=self.db_replicator.config.mysql,
                mysql_timezone=self.db_replicator.config.mysql_timezone,
            )
        
        if mysql_api is not None:
            try:
                # Get the CREATE statement for the source table
                source_create_statement = mysql_api.get_table_create_statement(source_table_name)
                
                # Parse the source table structure
                source_structure = self.parse_mysql_table_structure(source_create_statement)
                
                # Copy the structure but keep the new table name
                mysql_structure = copy.deepcopy(source_structure)
                mysql_structure.table_name = target_table_name
                
                if is_query_api:
                    # Convert to ClickHouse structure
                    ch_structure = self.convert_table_structure(mysql_structure)
                    return mysql_structure, ch_structure
                else:
                    return mysql_structure
                    
            except Exception as e:
                error_msg = f"Could not get source table structure for LIKE statement: {str(e)}"
                print(f"Error: {error_msg}")
                raise Exception(error_msg, create_statement)
            finally:
                if not mysql_api_was_provided:
                    mysql_api.close()
        
        # If we got here, we couldn't determine the structure
        raise Exception(f"Could not determine structure for source table '{source_table_name}' in LIKE statement", create_statement)

    def parse_create_table_query(self, mysql_query) -> tuple[TableStructure, TableStructure]:
        # Special handling for CREATE TABLE LIKE statements
        if 'LIKE' in mysql_query.upper():
            # Check if this is a CREATE TABLE LIKE statement using regex
            create_like_pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"]?([^`"\s]+)[`"]?\s+LIKE\s+[`"]?([^`"\s]+)[`"]?'
            match = re.search(create_like_pattern, mysql_query, re.IGNORECASE)
            
            if match:
                # This is a CREATE TABLE LIKE statement
                new_table_name = match.group(1).strip('`"')
                source_table_name = match.group(2).strip('`"')
                
                # Use the common helper method to handle the LIKE statement
                return self._handle_create_table_like(mysql_query, source_table_name, new_table_name, True)

        # Regular parsing for non-LIKE statements
        mysql_table_structure = self.parse_mysql_table_structure(mysql_query)
        ch_table_structure = self.convert_table_structure(mysql_table_structure)
        return mysql_table_structure, ch_table_structure

    def convert_drop_table_query(self, mysql_query):
        raise Exception('not implement')

    def _strip_comments(self, create_statement):
        """
        Strip COMMENT clauses from CREATE TABLE statements.
        Handles MySQL-style quote escaping where quotes are doubled ('' or "").
        
        This function properly parses SQL syntax to distinguish between:
        - COMMENT clauses (which should be removed)
        - String literals containing "COMMENT" (which should be preserved)
        - Identifiers containing "comment" (which should be preserved)
        """
        result = []
        i = 0
        
        while i < len(create_statement):
            char = create_statement[i]
            
            # Handle string literals (single quotes)
            if char == "'":
                result.append(char)
                i += 1
                # Copy the entire string literal, handling escaped quotes
                while i < len(create_statement):
                    char = create_statement[i]
                    result.append(char)
                    if char == "'":
                        # Check if this is an escaped quote (doubled)
                        if i + 1 < len(create_statement) and create_statement[i + 1] == "'":
                            i += 1  # Skip to the second quote
                            result.append(create_statement[i])  # Add the second quote
                        else:
                            i += 1  # End of string literal
                            break
                    i += 1
                continue
            
            # Handle string literals (double quotes)
            if char == '"':
                result.append(char)
                i += 1
                # Copy the entire string literal, handling escaped quotes
                while i < len(create_statement):
                    char = create_statement[i]
                    result.append(char)
                    if char == '"':
                        # Check if this is an escaped quote (doubled)
                        if i + 1 < len(create_statement) and create_statement[i + 1] == '"':
                            i += 1  # Skip to the second quote
                            result.append(create_statement[i])  # Add the second quote
                        else:
                            i += 1  # End of string literal
                            break
                    i += 1
                continue
            
            # Handle backtick-quoted identifiers
            if char == '`':
                result.append(char)
                i += 1
                # Copy the entire identifier
                while i < len(create_statement):
                    char = create_statement[i]
                    result.append(char)
                    if char == '`':
                        i += 1  # End of identifier
                        break
                    i += 1
                continue
            
            # Look for COMMENT keyword (case insensitive) outside of quotes
            if (i + 7 <= len(create_statement) and 
                create_statement[i:i+7].upper() == 'COMMENT' and
                (i == 0 or not create_statement[i-1].isalnum()) and
                (i + 7 >= len(create_statement) or not create_statement[i+7].isalnum())):
                
                # This looks like a COMMENT keyword, but we need to verify it's actually
                # a COMMENT clause and not just an identifier that happens to be "comment"
                
                # Skip COMMENT keyword
                j = i + 7
                
                # Skip whitespace and optional '='
                while j < len(create_statement) and create_statement[j].isspace():
                    j += 1
                if j < len(create_statement) and create_statement[j] == '=':
                    j += 1
                    while j < len(create_statement) and create_statement[j].isspace():
                        j += 1
                
                # Check if this is followed by a quoted string (indicating a COMMENT clause)
                if j < len(create_statement) and create_statement[j] in ('"', "'"):
                    # This is a COMMENT clause - skip it entirely
                    quote_char = create_statement[j]
                    j += 1  # Skip opening quote
                    
                    # Find the closing quote, handling escaped quotes
                    while j < len(create_statement):
                        if create_statement[j] == quote_char:
                            # Check if this is an escaped quote (doubled)
                            if j + 1 < len(create_statement) and create_statement[j + 1] == quote_char:
                                j += 2  # Skip both quotes
                            else:
                                j += 1  # Skip closing quote
                                break
                        else:
                            j += 1
                    
                    # Skip the entire COMMENT clause
                    i = j
                    continue
                else:
                    # This is not a COMMENT clause (no quoted string follows)
                    # Treat it as a regular identifier
                    result.append(char)
                    i += 1
                    continue
            
            # Regular character - just copy it
            result.append(char)
            i += 1
        
        return ''.join(result)

    def parse_mysql_table_structure(self, create_statement, required_table_name=None):
        create_statement = self._strip_comments(create_statement)

        structure = TableStructure()

        tokens = sqlparse.parse(create_statement.replace('\n', ' ').strip())[0].tokens
        tokens = [t for t in tokens if not t.is_whitespace and not t.is_newline]

        # remove "IF NOT EXISTS"
        if (len(tokens) > 5 and
                tokens[0].normalized.upper() == 'CREATE' and
                tokens[1].normalized.upper() == 'TABLE' and
                tokens[2].normalized.upper() == 'IF' and
                tokens[3].normalized.upper() == 'NOT' and
                tokens[4].normalized.upper() == 'EXISTS'):
            del tokens[2:5]  # Remove the 'IF', 'NOT', 'EXISTS' tokens
            structure.if_not_exists = True

        if tokens[0].ttype != sqlparse.tokens.DDL:
            raise Exception('wrong create statement', create_statement)
        if tokens[0].normalized.lower() != 'create':
            raise Exception('wrong create statement', create_statement)
        if tokens[1].ttype != sqlparse.tokens.Keyword:
            raise Exception('wrong create statement', create_statement)

        if not isinstance(tokens[2], sqlparse.sql.Identifier):
            raise Exception('wrong create statement', create_statement)

        # get_real_name() returns the table name if the token is in the
        # style `<dbname>.<tablename>`
        structure.table_name = strip_sql_name(tokens[2].get_real_name())

        # Handle CREATE TABLE ... LIKE statements
        if len(tokens) > 4 and tokens[3].normalized.upper() == 'LIKE':
            # Extract the source table name
            if not isinstance(tokens[4], sqlparse.sql.Identifier):
                raise Exception('wrong create statement', create_statement)
            
            source_table_name = strip_sql_name(tokens[4].get_real_name())
            target_table_name = strip_sql_name(tokens[2].get_real_name())
            
            # Use the common helper method to handle the LIKE statement
            return self._handle_create_table_like(create_statement, source_table_name, target_table_name, False)

        if not isinstance(tokens[3], sqlparse.sql.Parenthesis):
            raise Exception('wrong create statement', create_statement)

        #print(' --- processing statement:\n', create_statement, '\n')

        inner_tokens = tokens[3].tokens
        inner_tokens = ''.join([str(t) for t in inner_tokens[1:-1]]).strip()
        inner_tokens = split_high_level(inner_tokens, ',')

        prev_token = ''
        prev_prev_token = ''
        for line in tokens[4:]:
            curr_token = line.value
            if prev_token == '=' and prev_prev_token.lower() == 'charset':
                structure.charset = curr_token
            prev_prev_token = prev_token
            prev_token = curr_token

        structure.charset_python = 'utf-8'

        if structure.charset:
            structure.charset_python = CHARSET_MYSQL_TO_PYTHON[structure.charset]

        prev_line = ''
        for line in inner_tokens:
            line = prev_line + line
            q_count = line.count('`')
            if q_count % 2 == 1:
                prev_line = line
                continue
            prev_line = ''

            line = line.strip()
            line_lower = line.lower()

            if line_lower.startswith('constraint'):
                primary_key_match = re.search(r'\bprimary\s+key\b', line, re.IGNORECASE)
                if primary_key_match:
                    line = line[primary_key_match.start():]
                    line_lower = line.lower()
                else:
                    continue

            if line_lower.startswith('unique key'):
                continue
            if line_lower.startswith('unique index'):
                continue
            if line_lower.startswith('key'):
                continue
            if line_lower.startswith('index'):
                continue
            if line_lower.startswith('fulltext'):
                continue
            if line_lower.startswith('spatial'):
                continue
            # Handle unnamed UNIQUE constraints like "UNIQUE (uuid)" or "UNIQUE(uuid)"
            # This must be checked after other unique key checks to avoid false positives
            # We check if "unique" is followed by optional whitespace and then "("
            # This distinguishes constraints from a field named "unique" (which would be "unique VARCHAR(...)")
            if line_lower.startswith('unique') and len(line_lower) > 6:
                # Check if next non-space character after "unique" is "("
                remaining = line_lower[6:].lstrip()
                if remaining.startswith('('):
                    continue
            if line_lower.startswith('primary key'):
                # Define identifier to match column names, handling backticks and unquoted names
                identifier = (Suppress('`') + Word(alphas + alphanums + '_') + Suppress('`')) | Word(
                    alphas + alphanums + '_')

                # Build the parsing pattern
                pattern = CaselessKeyword('PRIMARY') + CaselessKeyword('KEY') + Suppress('(') + delimitedList(
                    identifier)('column_names') + Suppress(')')

                # Parse the line
                result = pattern.parseString(line)

                # Extract and process the primary key column names
                primary_keys = [strip_sql_name(name) for name in result['column_names']]

                structure.primary_keys = primary_keys

                continue

            # print(" === processing line", line)

            if line.startswith('`'):
                end_pos = line.find('`', 1)
                field_name = line[1:end_pos]
                line = line[end_pos + 1 :].strip()
                # Use our new enum parsing utilities
                field_name, field_type, field_parameters = parse_enum_or_set_field(line, field_name, is_backtick_quoted=True)
            else:
                definition = line.split(' ')
                field_name = strip_sql_name(definition[0])
                # Use our new enum parsing utilities
                field_name, field_type, field_parameters = parse_enum_or_set_field(line, field_name, is_backtick_quoted=False)

            # Extract additional data for enum and set types
            additional_data = extract_enum_or_set_values(field_type, from_parser_func=parse_mysql_enum)

            structure.fields.append(TableField(
                name=field_name,
                field_type=field_type,
                parameters=field_parameters,
                additional_data=additional_data,
            ))
            #print(' ---- params:', field_parameters)


        if not structure.primary_keys:
            for field in structure.fields:
                if 'primary key' in field.parameters.lower():
                    structure.primary_keys.append(field.name)

        if not structure.primary_keys:
            if structure.has_field('id'):
                structure.primary_keys = ['id']

        if not structure.primary_keys:
            raise Exception(f'No primary key for table {structure.table_name}, {create_statement}')

        structure.preprocess()
        return structure
