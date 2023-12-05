import psycopg2
import requests


def get_employers() -> list[dict]:
    """Получает список работодателей с сайта hh.ru"""
    params = {}
    headers = {'User-Agent': 'GetVacancies/1.0'}
    res = requests.get('https://api.hh.ru/employers', headers=headers, params=params)
    count_employers = res.json()['found']
    res.close()

    i = 0
    emp_list = []
    while i < count_employers and len(emp_list) < 20:
        res = requests.get('https://api.hh.ru/employers/' + str(i + 1))
        json_data: dict = res.json()
        emp_dict = {}
        count_vacancies = json_data['open_vacancies'] if ('open_vacancies' in json_data and
                                                          json_data['open_vacancies'] is not None) else 0
        if count_vacancies > 10:
            emp_dict = {
                'id': json_data['id'],
                'name': json_data['name'],
                'type': json_data['type'],
                'description': json_data['description'],
                'location': json_data['area']['name']
            }
            emp_list.append(emp_dict)
        res.close()
        i += 1
    return emp_list


def get_vacancies(emp_id: int) -> list[dict]:
    """
    Получает список вакансий указанного работодателя
    @param emp_id: id работодателя
    @return: список вакансий
    """
    params = {
        'employer_id': emp_id,
        'page': 0,
        'per_page': 50
    }
    res = requests.get('https://api.hh.ru/vacancies', params)
    json_data = res.json()
    res.close()

    vacancies = []
    for item in json_data['items']:
        vacancy_dict = {
            'name': item['name'],
            'department': item['department']['name'] if 'department' in item and item[
                'department'] is not None else None,
            'url': item['url'],
            'salary': item['salary']['from'] if 'salary' in item and item['salary'] is not None and
                                                'from' in item['salary'] else None,
            'requirement': item['snippet']['requirement'],
            'responsibility': item['snippet']['responsibility']
        }
        vacancies.append(vacancy_dict)
    return vacancies


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {db_name} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {db_name}")
    conn.close()

    # params.update({'dbname': db_name})
    conn = psycopg2.connect(dbname=db_name, **params)
    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE employers(
                    employer_id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    type VARCHAR(100) NOT NULL,
                    description TEXT,
                    location VARCHAR(100) NOT NULL
                )     
        """)
        cur.execute("""
                CREATE TABLE vacancies(
                    vacancy_id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    department VARCHAR(100),
                    url VARCHAR(100) NOT NULL,
                    salary INT,
                    requirement TEXT,
                    responsibility TEXT,
                    employer_id INT NOT NULL REFERENCES employers(employer_id)
                )
        """)
    conn.commit()
    conn.close()


def fill_database(params, db_name) -> None:
    """Добавляет данные о работодателях и вакансиях в БД"""
    employers = get_employers()

    conn = psycopg2.connect(dbname=db_name, **params)
    with conn.cursor() as cur:
        for emp in employers:
            cur.execute("INSERT INTO employers(name, type, description, location) VALUES(%s, %s, %s, %s) "
                        "RETURNING employer_id",
                        (emp['name'], emp['type'], emp['description'], emp['location']))
            employer_id = cur.fetchone()[0]

            vacancies = get_vacancies(emp['id'])
            list_of_tuples = []
            for vacancy in vacancies:
                vacancy['employer_id'] = employer_id
                list_of_tuples.append(tuple(vacancy.values()))
            cur.executemany("INSERT INTO "
                            "vacancies(name, department, url, salary, requirement, responsibility, employer_id) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s)", list_of_tuples)
    conn.commit()
    conn.close()
