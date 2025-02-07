import datetime
import re
import json
from enum import Enum
import os


class DataType(Enum):
    INTEGER = 'INTEGER'
    FLOAT = 'FLOAT'
    DATE = 'DATE'
    TIME = 'TIME'
    CHAR = 'CHAR'
    BINARY = 'BINARY'


class Table:
    def __init__(self, name, schema):
        """
        schema: dict, key=column name, value=DataType
        """
        self.name = name
        self.schema = schema
        self.rows = []

    def insert(self, record):
        # Validate and insert the record
        if set(record.keys()) != set(self.schema.keys()):
            raise ValueError("The columns of the record do not match the schema of the table")
        
        for column, value in record.items():
            expected_type = self.schema[column]
            if not self.validate_type(value, expected_type):
                raise TypeError(f"Error {column} should be {expected_type.value}.")
        
        self.rows.append(record)

    def select(self, columns=None, where=None, order_by=None):
        # Select records based on columns, where clause, and order by
        if columns is None or columns == ['*']:
            columns = list(self.schema.keys())
        
        result = []
        for row in self.rows:
            if where is None or where(row):
                selected_row = {col: row[col] for col in columns}
                result.append(selected_row)
        
        # 处理ORDER BY
        if order_by:
            for ob in reversed(order_by):
                col = ob['column']
                direction = ob['direction']
                reverse = True if direction == 'DESC' else False
                result.sort(key=lambda x: x[col], reverse=reverse)
        
        return result

    def update(self, updates, where=None):
        # Update records based on where clause
        count = 0
        for row in self.rows:
            if where is None or where(row):
                for column, value in updates.items():
                    if column in self.schema:
                        expected_type = self.schema[column]
                        if not self.validate_type(value, expected_type):
                            raise TypeError(f"Error:  {column} should be {expected_type.value}.")
                        row[column] = value
                count += 1
        return count

    def delete(self, where=None):
        # Delete records based on where clause
        original_length = len(self.rows)
        self.rows = [row for row in self.rows if not (where(row) if where else False)]
        return original_length - len(self.rows)

    def validate_type(self, value, data_type):
        if data_type == DataType.INTEGER:
            return isinstance(value, int)
        elif data_type == DataType.FLOAT:
            return isinstance(value, float)
        elif data_type == DataType.DATE:
            return isinstance(value, datetime.date)
        elif data_type == DataType.TIME:
            return isinstance(value, datetime.time)
        elif data_type == DataType.CHAR:
            return isinstance(value, str)
        elif data_type == DataType.BINARY:
            return isinstance(value, bytes)
        return False

    def to_dict(self):
        # Convert table to a serializable dictionary
        return {
            'name': self.name,
            'schema': {col: dtype.value for col, dtype in self.schema.items()},
            'rows': self.rows
        }

    @staticmethod
    def from_dict(data):
        # Create a Table instance from a dictionary
        schema = {col: DataType(dtype) for col, dtype in data['schema'].items()}
        table = Table(data['name'], schema)
        table.rows = data['rows']
        return table

class Database:
    def __init__(self):
        self.tables = {}

    def create_table(self, name, schema):
        if name in self.tables:
            raise ValueError(f"Table {name} exists")
        self.tables[name] = Table(name, schema)

    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]
        else:
            raise ValueError(f"List {name} does not exist.")

    def to_dict(self):
        # Convert database to a serializable dictionary
        return {
            'tables': {name: table.to_dict() for name, table in self.tables.items()}
        }

    def save_to_file(self, filename='lite_mysql_db.json'):
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.to_dict(), f, ensure_ascii=False, indent=4, default=str)
            print(f"The database has been saved to {filename}")
        except Exception as e:
            print(f"Error saving database {e}")

    def load_from_file(self, filename='lite_mysql_db.json'):
        if not os.path.exists(filename):
            print(f"Database file not found {filename}，A new database will be created.")
            return
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for table_name, table_data in data.get('tables', {}).items():
                table = Table.from_dict(table_data)
                self.tables[table_name] = table
            print(f" Loaded from {filename} ")
        except Exception as e:
            print(f"Error loading database {e}")


class LiteMySQL:
    def __init__(self):
        self.db = Database()
        self.db.load_from_file()

    def execute(self, query):
        query = query.strip().rstrip(';').strip()
        if not query:
            return
        command = query.split()[0].upper()

        if command == 'CREATE':
            self.handle_create(query)
        elif command == 'INSERT':
            self.handle_insert(query)
        elif command == 'SELECT':
            self.handle_select(query)
        elif command == 'UPDATE':
            self.handle_update(query)
        elif command == 'DELETE':
            self.handle_delete(query)
        elif command == 'DROP':
            self.handle_drop(query)
        elif command in ['EXIT', 'QUIT']:
            self.db.save_to_file()
            print("Exit LiteMySQL")
            exit(0)
        else:
            print(f"Unsupported commands: {command}")

    def handle_create(self, query):

        pattern = r'CREATE\s+TABLE\s+(\w+)\s*\((.+)\)'
        match = re.match(pattern, query, re.IGNORECASE)
        if not match:
            print("CREATE TABLE grammatical error")
            return
        table_name = match.group(1)
        columns_str = match.group(2)

        columns_parts = self.split_columns_and_other_definitions(columns_str)
        
        schema = {}
        for part in columns_parts:
            part = part.strip()

            if part.upper().startswith('INDEX'):
                print("Error")
                return
            else:

                parts = part.split()
                if len(parts) != 2:
                    print(f"列定义Error: {part}")
                    return
                col_name, col_type = parts
                col_type = col_type.upper()
                if col_type not in DataType.__members__:
                    print(f"Unsupported data types:  {col_type}")
                    return
                schema[col_name] = DataType[col_type]
        
        try:
            self.db.create_table(table_name, schema)
            print(f"Table {table_name} created")
            self.db.save_to_file()
        except ValueError as ve:
            print(f"Error: {ve}")

    def split_columns_and_other_definitions(self, columns_str):

        parts = []
        current = ''
        paren_level = 0
        for char in columns_str:
            if char == '(':
                paren_level += 1
                current += char
            elif char == ')':
                paren_level -= 1
                current += char
            elif char == ',' and paren_level == 0:
                parts.append(current)
                current = ''
            else:
                current += char
        if current:
            parts.append(current)
        return parts

    def handle_insert(self, query):

        pattern = r'INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s+VALUES\s*\(([^)]+)\)'
        match = re.match(pattern, query, re.IGNORECASE)
        if not match:
            print("INSERT grammatical error")
            return
        table_name = match.group(1)
        columns = [col.strip() for col in match.group(2).split(',')]
        values = [self.parse_value(val.strip()) for val in self.split_values(match.group(3))]
        if len(columns) != len(values):
            print("Mismatch in the number of columns and values")
            return
        record = dict(zip(columns, values))
        try:
            if table_name not in self.db.tables:
                print(f"Table {table_name} does not exist")
                return
            self.db.tables[table_name].insert(record)
            print(f"Records have been inserted into the table {table_name}")
            self.db.save_to_file()
        except (ValueError, TypeError) as e:
            print(f"Error: {e}")

    def handle_select(self, query):

        pattern = r'SELECT\s+(.+)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+))?$'
        match = re.match(pattern, query, re.IGNORECASE)
        if not match:
            print("SELECT grammatical error")
            return
        columns_str, table_name, where_str, order_by_str = match.groups()
        columns = [col.strip() for col in columns_str.split(',')] if columns_str != '*' else ['*']
        where = self.parse_where(where_str) if where_str else None
        order_by = self.parse_order_by(order_by_str) if order_by_str else None
        try:
            if table_name not in self.db.tables:
                print(f"Table {table_name} does not exist")
                return
            results = self.db.tables[table_name].select(columns, where, order_by)
            if results:

                headers = results[0].keys()
                print("\t".join(headers))

                for row in results:
                    print("\t".join(str(value) for value in row.values()))
            else:
                print("No eligible records")
        except Exception as e:
            print(f"Error: {e}")

    def handle_update(self, query):
        pattern = r'UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$'
        match = re.match(pattern, query, re.IGNORECASE)
        if not match:
            print("UPDATE grammatical error")
            return
        table_name, set_str, where_str = match.groups()
        updates = {}
        assignments = self.split_assignments(set_str)
        for assign in assignments:
            parts = assign.split('=')
            if len(parts) != 2:
                print(f"赋值 grammatical error: {assign}")
                return
            col, val = parts
            col = col.strip()
            val = self.parse_value(val.strip())
            updates[col] = val
        where = self.parse_where(where_str) if where_str else None
        try:
            if table_name not in self.db.tables:
                print(f"Table {table_name} does not exist")
                return
            count = self.db.tables[table_name].update(updates, where)
            print(f"Updated {count} records.")
            self.db.save_to_file()
        except (ValueError, TypeError) as e:
            print(f"Error: {e}")

    def handle_delete(self, query):

        pattern = r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?'
        match = re.match(pattern, query, re.IGNORECASE)
        if not match:
            print("DELETE grammatical error")
            return
        table_name, where_str = match.groups()
        where = self.parse_where(where_str) if where_str else None
        try:
            if table_name not in self.db.tables:
                print(f"Table {table_name} does not exist")
                return
            count = self.db.tables[table_name].delete(where)
            print(f"Deleted {count} records.")
            self.db.save_to_file()
        except Exception as e:
            print(f"Error: {e}")

    def handle_drop(self, query):

        pattern = r'DROP\s+TABLE\s+(\w+)'
        match = re.match(pattern, query, re.IGNORECASE)
        if not match:
            print("DROP TABLE grammatical error")
            return
        table_name = match.group(1)
        try:
            self.db.drop_table(table_name)
            print(f"Table {table_name} deleted.")
            self.db.save_to_file()
        except ValueError as ve:
            print(f"Error: {ve}")

    def parse_where(self, condition_str):

        if not condition_str:
            return None
        condition_str = condition_str.strip()
        match = re.match(r'(\w+)\s*=\s*(.+)', condition_str)
        if not match:
            print("col = value only")
            return None
        col, val_str = match.groups()
        val = self.parse_value(val_str.strip())
        return lambda row: row.get(col) == val

    def parse_order_by(self, order_by_str):
        order_by_parts = self.split_order_by(order_by_str)
        order_by = []
        for part in order_by_parts:
            parts = part.strip().split()
            col = parts[0]
            direction = 'ASC'
            if len(parts) > 1:
                if parts[1].upper() in ['ASC', 'DESC']:
                    direction = parts[1].upper()
            order_by.append({'column': col, 'direction': direction})
        return order_by

    def split_order_by(self, order_by_str):
        parts = []
        current = ''
        paren_level = 0
        for char in order_by_str:
            if char == '(':
                paren_level += 1
                current += char
            elif char == ')':
                paren_level -= 1
                current += char
            elif char == ',' and paren_level == 0:
                parts.append(current)
                current = ''
            else:
                current += char
        if current:
            parts.append(current)
        return parts

    def split_assignments(self, set_str):
        parts = []
        current = ''
        in_quotes = False
        for char in set_str:
            if char == "'" and not in_quotes:
                in_quotes = True
                current += char
            elif char == "'" and in_quotes:
                in_quotes = False
                current += char
            elif char == ',' and not in_quotes:
                parts.append(current)
                current = ''
            else:
                current += char
        if current:
            parts.append(current)
        return parts

    def parse_value(self, value_str):
        if re.match(r'^-?\d+$', value_str):
            return int(value_str)
        elif re.match(r'^-?\d+\.\d+$', value_str):
            return float(value_str)
        elif re.match(r"^'.*'$", value_str):
            return value_str.strip("'")
        else:
            try:
                return datetime.datetime.strptime(value_str, "%Y-%m-%d").date()
            except ValueError:
                pass
            try:
                return datetime.datetime.strptime(value_str, "%H:%M:%S").time()
            except ValueError:
                pass

            return value_str

    def split_values(self, values_str):
        values = []
        current = ''
        in_quotes = False
        for char in values_str:
            if char == "'" and not in_quotes:
                in_quotes = True
                current += char
            elif char == "'" and in_quotes:
                in_quotes = False
                current += char
            elif char == ',' and not in_quotes:
                values.append(current)
                current = ''
            else:
                current += char
        if current:
            values.append(current)
        return values


def interactive_cli():
    lite_mysql = LiteMySQL()
    print("LiteMySQL command line interface.")
    print("Enter a SQL command and type EXIT or QUIT to exit.")
    while True:
        try:
            query = input("LiteMySQL> ")
            lite_mysql.execute(query)
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    interactive_cli()
