import requests
from api_handler import APIHandler
from db_manager import DBManager


def fetch_company_info(company_id):
    """
    Получает информацию о компании по её ID.

    :param company_id: Идентификатор компании
    :return: Информация о компании в формате JSON или None в случае ошибки
    """
    api_url = f"https://api.hh.ru/employers/{company_id}"
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Проверяем успешность выполнения запроса
        data = response.json()
        return data  # Возвращаем информацию о компании
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении информации о компании ID {company_id}: {e}")
        return None


def fetch_vacancies(company_id):
    """
    Получает список вакансий для указанной компании по её ID.

    :param company_id: Идентификатор компании
    :return: Список вакансий или пустой список в случае ошибки
    """
    api_url = f"https://api.hh.ru/vacancies?employer_id={company_id}"
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Проверяем успешность выполнения запроса
        data = response.json()
        if 'items' in data:
            return data['items']  # Возвращаем список вакансий
        else:
            return []  # Если вакансий нет, возвращаем пустой список
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении вакансий для компании ID {company_id}: {e}")
        return []


def main():
    # Параметры подключения к базе данных
    db_params = {
        'dbname': 'bd_hh',
        'user': 'postgres',
        'password': 'SbOg10Dk',
        'host': 'localhost',
        'port': '5432'
    }

    db_manager = DBManager(db_params)

    # Создание базы данных и подключение к ней
    db_created = db_manager.create_database()  # Функция возвращает True, если база данных создана, и False, если она уже существовала
    db_manager.connect()  # Подключение к базе данных

    # Проверяем, была ли база данных только что создана
    if db_created:
        # Если база данных создана, создаем таблицы сразу
        db_manager.create_tables()
        print("Таблицы созданы.")
    else:
        # Если база данных уже существовала, спрашиваем об удалении таблиц
        delete_choice = input("Хотите удалить существующие таблицы? (да/нет): ")
        if delete_choice.lower() == 'да':
            db_manager.delete_tables()  # Удаление таблиц
            db_manager.create_tables()  # Создание таблиц сразу после удаления
            print("Таблицы удалены и созданы заново.")
        else:
            db_manager.create_tables()  # Создание таблиц без удаления
            print("Таблицы созданы.")

    # Продолжение работы с API и базой данных
    companies = [
        {'id': '1740', 'name': 'Яндекс'},
        {'id': '3529', 'name': 'Сбер'},
        {'id': '15478', 'name': 'VK'},
        {'id': '3776', 'name': 'МТС'},
        {'id': '80', 'name': 'Альфабанк'},
        {'id': '1122462', 'name': 'Skyeng'},
        {'id': '78638', 'name': 'Тинькоф'},
        {'id': '2628254', 'name': 'Code class'},
        {'id': '8884', 'name': 'DrWeb'}
    ]

    api_handler = APIHandler("https://api.hh.ru")

    for company in companies:
        company_id = company['id']
        company_name = company['name']

        db_company_id = db_manager.insert_company(company_name)
        print(f"\nЗагружаем вакансии для компании: {company_name}")

        try:
            vacancies_data = api_handler.fetch_data(f"vacancies?employer_id={company_id}")
            if vacancies_data:
                vacancies = vacancies_data.get('items', [])  # Извлечение списка вакансий
                if vacancies:
                    for vacancy in vacancies:
                        salary_info = vacancy.get('salary', {})
                        salary = salary_info.get('from', 0) if isinstance(salary_info, dict) else 0
                        db_manager.insert_vacancy(vacancy['name'], salary, db_company_id)
                    print(f"Вакансии для компании {company_name} загружены успешно.")
                else:
                    print(f"Вакансии для компании {company_name} не найдены.")
            else:
                print(f"Ошибка: Не удалось получить вакансии для компании ID {company_id}.")
        except requests.exceptions.HTTPError as e:
            print(f"Ошибка при получении данных для компании ID {company_id}: {e}")

    # Меню для вывода данных
    while True:
        print("\nВыберите действие:")
        print("1. Показать компании и количество вакансий")
        print("2. Показать все вакансии")
        print("3. Показать среднюю зарплату")
        print("4. Показать вакансии с зарплатой выше средней")
        print("5. Поиск вакансий по ключевому слову")
        print("6. Выход")

        choice = input("Введите номер действия: ").strip()

        if choice == '1':
            print("\nКомпании и количество вакансий:")
            for company in db_manager.get_companies_and_vacancies_count():
                print(f"Компания: {company[0]}, Количество вакансий: {company[1]}")

        elif choice == '2':
            print("\nВсе вакансии:")
            for vacancy in db_manager.get_all_vacancies():
                print(f"Вакансия: {vacancy[0]}, Зарплата: {vacancy[2]}, Компания: {vacancy[1]}")

        elif choice == '3':
            avg_salary = db_manager.get_avg_salary()
            print(f"\nСредняя зарплата: {round(avg_salary)}")

        elif choice == '4':
            print("\nВакансии с зарплатой выше средней:")
            vacancies = db_manager.get_vacancies_with_higher_salary()
            for vacancy in vacancies:
                # vacancy содержит только два элемента: название вакансии и зарплату
                print(f"Вакансия: {vacancy[0]}, Зарплата: {vacancy[1]}")

        elif choice == '5':
            keyword = input("Введите ключевое слово для поиска вакансий: ").strip()
            if keyword:
                print(f"\nВакансии с ключевым словом '{keyword}':")
                for vacancy in db_manager.get_vacancies_with_keyword(keyword):
                    print(f"Вакансия: {vacancy[0]}, Компания: {vacancy[1]}, Зарплата: {vacancy[2]}")

        elif choice == '6':
            print("Выход из программы.")
            break

        else:
            print("Неверный ввод. Попробуйте снова.")

    db_manager.close()


if __name__ == "__main__":
    main()
