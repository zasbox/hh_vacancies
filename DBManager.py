import psycopg2


class DBManager:
    def __init__(self, params):
        self.__conn = psycopg2.connect(**params)

    def get_companies_and_vacancies_count(self) -> list:
        """получает список всех компаний и количество вакансий у каждой компании"""
        employers = []
        with self.__conn.cursor() as cur:
            cur.execute("SELECT employers.name, COUNT(vacancies.vacancy_id) "
                        "FROM employers JOIN vacancies USING(employer_id) GROUP BY employers.name")
            employers = cur.fetchall()
        return employers

    def get_all_vacancies(self) -> list:
        """получает список вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию"""
        vacancies = []
        with self.__conn.cursor() as cur:
            cur.execute("SELECT employers.name, vacancies.name, vacancies.salary, vacancies.url "
                        "FROM employers JOIN vacancies USING(employer_id)")
            vacancies = cur.fetchall()
        return vacancies

    def get_avg_salary(self) -> int:
        """получает среднюю зарплату по вакансиям"""
        avg_salary = 0
        with self.__conn.cursor() as cur:
            cur.execute("SELECT ROUND(AVG(salary)) FROM vacancies")
            avg_salary = cur.fetchone()[0]
        return avg_salary

    def get_vacancies_with_higher_salary(self) -> list:
        """получает список всех вакансий, у которых зарплата выше средней по всем вакансиям"""
        with self.__conn.cursor() as cur:
            cur.execute("SELECT employers.name, vacancies.name, vacancies.salary, vacancies.url FROM employers JOIN "
                        "vacancies USING(employer_id) WHERE vacancies.salary > (SELECT AVG(salary) FROM vacancies);")
            vacancies = cur.fetchall()
        return vacancies

    def get_vacancies_with_keyword(self, keyword: str) -> list:
        """получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        with self.__conn.cursor() as cur:
            cur.execute(f"SELECT employers.name, vacancies.name, vacancies.salary, vacancies.url FROM employers JOIN "
                        f"vacancies USING(employer_id) WHERE vacancies.name LIKE '%{keyword}%'")
            vacancies = cur.fetchall()
        return vacancies

