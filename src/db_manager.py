import psycopg2
from psycopg2 import sql
from typing import List, Dict, Tuple, Union


class DBManager:
    def __init__(self, db_params: Dict[str, str]):
        """
        Инициализирует объект DBManager и устанавливает соединение с базой данных.

        :param db_params: Словарь параметров подключения к базе данных. Ключи: 'dbname', 'user', 'password', 'host', 'port'.
        """
        self.db_params = db_params
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Подключение к базе данных.
        """
        try:
            self.connection = psycopg2.connect(**self.db_params)
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            print("Успешное подключение к базе данных.")
        except psycopg2.DatabaseError as e:
            print(f"Ошибка подключения к базе данных: {e}")

    def create_database(self):
        """
        Создание базы данных, если она не существует.
        """
        try:
            # Подключаемся к системной БД PostgreSQL
            conn = psycopg2.connect(
                dbname='postgres',
                user=self.db_params['user'],
                password=self.db_params['password'],
                host=self.db_params['host'],
                port=self.db_params['port']
            )
            conn.autocommit = True
            cur = conn.cursor()
            # Проверка на существование базы данных и её создание
            cur.execute(sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"), [self.db_params['dbname']])
            if not cur.fetchone():
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.db_params['dbname'])))
                print(f"База данных '{self.db_params['dbname']}' успешно создана.")
            else:
                print(f"База данных '{self.db_params['dbname']}' уже существует.")
            cur.close()
            conn.close()
        except psycopg2.Error as e:
            print(f"Ошибка при создании базы данных: {e}")

    def delete_tables(self):
        """
        Удаляет таблицы из базы данных.
        """
        try:
            # Удаляем таблицы, если они существуют
            self.cursor.execute("DROP TABLE IF EXISTS vacancies")
            self.cursor.execute("DROP TABLE IF EXISTS companies")
            self.connection.commit()
            print("Таблицы успешно удалены.")
        except Exception as e:
            print(f"Ошибка при удалении таблиц: {e}")
            self.connection.rollback()

    def create_tables(self):
        """
        Создает таблицы 'companies' и 'vacancies', если они еще не существуют.
        """
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                salary INTEGER,
                company_id INTEGER REFERENCES companies(id)
            )
        """)
        self.connection.commit()

    def insert_company(self, name: str) -> int:
        """
        Вставляет новую компанию в таблицу 'companies'.

        :param name: Название компании.
        :return: Идентификатор вставленной компании.
        """
        self.cursor.execute("INSERT INTO companies (name) VALUES (%s) RETURNING id", (name,))
        company_id = self.cursor.fetchone()[0]
        self.connection.commit()
        return company_id

    def insert_vacancy(self, title: str, salary: int, company_id: int):
        """
        Вставляет новую вакансию в таблицу 'vacancies'.

        :param title: Название вакансии.
        :param salary: Зарплата по вакансии.
        :param company_id: Идентификатор компании, к которой относится вакансия.
        """
        self.cursor.execute("INSERT INTO vacancies (title, salary, company_id) VALUES (%s, %s, %s)",
                            (title, salary, company_id))
        self.connection.commit()

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """
        Получает список всех компаний и количество вакансий у каждой компании.

        :return: Список кортежей, где каждый кортеж содержит название компании и количество вакансий.
        """
        self.cursor.execute("""
            SELECT c.name, COUNT(v.id) FROM companies c
            LEFT JOIN vacancies v ON c.id = v.company_id
            GROUP BY c.name
        """)
        return self.cursor.fetchall()

    def get_all_vacancies(self) -> List[Tuple[str, str, Union[int, None]]]:
        """
        Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты.

        :return: Список кортежей, где каждый кортеж содержит название вакансии, название компании и зарплату.
        """
        self.cursor.execute("""
            SELECT v.title, c.name, v.salary FROM vacancies v
            JOIN companies c ON v.company_id = c.id
        """)
        return self.cursor.fetchall()

    def get_avg_salary(self) -> float:
        """
        Получает среднюю зарплату по всем вакансиям.

        :return: Средняя зарплата.
        """
        self.cursor.execute("SELECT AVG(salary) FROM vacancies")
        return self.cursor.fetchone()[0]

    def get_vacancies_with_higher_salary(self) -> List[Tuple[str, int, str]]:
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.

        :return: Список кортежей, где каждый кортеж содержит название вакансии, зарплату и название компании.
        """
        avg_salary = self.get_avg_salary()
        self.cursor.execute("""
            SELECT v.title, v.salary, c.name FROM vacancies v
            JOIN companies c ON v.company_id = c.id
            WHERE v.salary > %s
        """, (avg_salary,))
        return self.cursor.fetchall()

    def get_vacancies_with_keyword(self, keyword: str) -> List[Tuple[str, str, int]]:
        """
        Получает список всех вакансий, в названии которых содержится переданное слово (ключевое слово).

        :param keyword: Ключевое слово для поиска.
        :return: Список кортежей, где каждый кортеж содержит название вакансии, название компании и зарплату.
        """
        self.cursor.execute("""
            SELECT v.title, c.name, v.salary
            FROM vacancies v
            JOIN companies c ON v.company_id = c.id
            WHERE v.title ILIKE %s
        """, (f"%{keyword}%",))
        results = self.cursor.fetchall()
        print("Таких вакансий не найдено")
        return results

    def close(self):
        """
        Закрывает курсор и соединение с базой данных.
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Подключение к базе данных закрыто.")