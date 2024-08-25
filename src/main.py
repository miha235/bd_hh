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

    # Спрашиваем пользователя, хочет ли он удалить таблицы
    delete_choice = input("Хотите удалить существующие таблицы? (да/нет): ")
    if delete_choice.lower() == 'да':
        db_manager.delete_tables()  # Удаление таблиц
        db_manager.create_tables()  # Создание таблиц сразу после удаления

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
        print(f"Загружаем вакансии для компании: {company_name}")

        try:
            vacancies_data = api_handler.fetch_data(f"vacancies?employer_id={company_id}")
            vacancies = vacancies_data.get('items', [])  # Извлечение списка вакансий
            if isinstance(vacancies, list):  # Проверка, что 'vacancies' - это список
                for vacancy in vacancies:
                    if isinstance(vacancy, dict):  # Проверка, что 'vacancy' - это словарь
                        salary_info = vacancy.get('salary', {})
                        if isinstance(salary_info, dict):
                            salary = salary_info.get('from', 0)  # Получаем зарплату из вложенного словаря
                        else:
                            salary = 0  # Если 'salary' не словарь, устанавливаем зарплату в 0

                        db_manager.insert_vacancy(vacancy['name'], salary, db_company_id)
                    else:
                        print(f"Ожидался словарь, но получен другой тип: {type(vacancy)}")
            else:
                print(f"Ожидался список, но получен другой тип: {type(vacancies)}")
        except requests.exceptions.HTTPError as e:
            print(f"Ошибка при получении данных для компании ID {company_id}: {e}")

    # Пример вывода данных
    print("Компании и количество вакансий:")
    for company in db_manager.get_companies_and_vacancies_count():
        print(company)

    print("\nВсе вакансии:")
    for vacancy in db_manager.get_all_vacancies():
        print(vacancy)

    print("\nСредняя зарплата:")
    print(db_manager.get_avg_salary())

    print("\nВакансии с зарплатой выше средней:")
    for vacancy in db_manager.get_vacancies_with_higher_salary():
        print(vacancy)

    # Запрашиваем у пользователя ключевое слово для поиска вакансий
    keyword = input("\nВведите ключевое слово для поиска вакансий: ").strip()
    if keyword:
        print(f"\nВакансии с ключевым словом '{keyword}':")
        for vacancy in db_manager.get_vacancies_with_keyword(keyword):
            print(vacancy)

    db_manager.close()


if __name__ == "__main__":
    main()
