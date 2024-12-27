import sys

from model import Model
from view import View


class Controller:
    def __init__(self):
        self.view = View()
        try:
            self.model = Model()
            self.view.show_message("Підключено до бази даних")
        except Exception as e:
            self.view.show_message(f"Сталася помилка під час ініціалізації: {e}")
            sys.exit(1)

    def run(self):
        while True:
            choice = self.view.show_menu()
            if choice == '1':
                self.view_tables()
            elif choice == '2':
                self.view_table()
            elif choice == '3':
                self.add_data()
            elif choice == '4':
                self.update_data()
            elif choice == '5':
                self.delete_data()
            elif choice == '6':
                self.generate_data()
            elif choice == '7':
                self.search_data()
            elif choice == '8':
                break

    def view_tables(self):
        tables = self.model.get_all_tables()
        self.view.show_tables(tables)

    def view_table(self):
        while True:
            table_name = self.view.ask_table()

            self.model.print_table(table_name)
            agree = self.view.ask_continue()
            if agree == 'n':
                break

    def add_data(self):
        while True:
            table, columns, val = self.view.insert()
            error = self.model.add_data(table, columns, val)
            if int(error) == 1:
                self.view.show_message("Дані додано!")

            elif int(error) == 2:
                self.view.show_message("Заданий унікальний id вже існує!")

            elif int(error) == 3:
                self.view.show_message("Недійсний зовнішній ключ")
            else:
                self.view.show_message(f"Невдалось оновити дані")
            agree = self.view.ask_continue()
            if agree == 'n':
                break

    def update_data(self):
        while True:
            table, column, id, new_value = self.view.update()
            error = self.model.update_data(table, column, id, new_value)
            if int(error) == 1:
                self.view.show_message("Дані оновлено!")
            elif int(error) == 2:
                self.view.show_message(f"Унікальний id  {new_value} вже існує!")
            elif int(error) == 3:
                self.view.show_message(f"Недійсний зовнішній ключ {new_value} у колонці {column}")
            else:
                self.view.show_message(f"Невдалось оновити дані")
            agree = self.view.ask_continue()
            if agree == 'n':
                break

    def delete_data(self):
        while True:
            table, id = self.view.delete()
            error = self.model.delete_data(table, id)
            if int(error) == 1:
                self.view.show_message("Рядок видалено!")
            elif int(error) == 0:
                self.view.show_message("Неможливо видалити рядок, оскільки існують зв\'язані дані")
            else:
                self.view.show_message(f"Невдалось видалити дані")
            agree = self.view.ask_continue()
            if agree == 'n':
                break

    def generate_data(self):
        while True:
            table_name, num_rows = self.view.generate_data_input()
            error = self.model.generate_data(table_name, num_rows)


            if int(error) == 1:
                self.view.show_message(f"Дані для таблиці {table_name} були згенеровані")
            else:
                self.view.show_message(f"Невдалось згенрувати дані")
            agree = self.view.ask_continue()
            if agree == 'n':
                break

    def search_data(self):
        while True:
            table, stable, row_name, row_data, group_name = self.view.search_input()
            error = self.model.search_data(table, stable, row_name, row_data, group_name)

            if int(error) == 1:
                self.view.show_message(f"Дані знайдено")
            else:
                self.view.show_message(f"При пошуку данних сталася помилка")
            agree = self.view.ask_continue()
            if agree == 'n':
                break
