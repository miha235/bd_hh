import requests

class APIHandler:
    def __init__(self, api_url: str):
        """
        Инициализирует объект APIHandler с базовым URL для API.

        :param api_url: Базовый URL для API.
        """
        self.api_url = api_url

    def fetch_data(self, endpoint: str) -> dict:
        """
        Получает данные с API по заданному endpoint.

        :param endpoint: Endpoint API, к которому нужно обратиться.
        :return: Ответ от API в формате JSON, преобразованный в словарь Python.
        :raises HTTPError: Если запрос к API завершился неудачей (статус код ошибки).
        """
        response = requests.get(f"{self.api_url}/{endpoint}")
        response.raise_for_status()  # Проверка на успешное выполнение запроса
        return response.json()  # Возвращает данные в формате словаря
