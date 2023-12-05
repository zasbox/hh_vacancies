from DBManager import DBManager
from config import config


def main():
    db_name = 'hh_vacancies_db'
    params = config()

    # create_database(params, db_name)
    # fill_database(params, db_name)

    params.update({'dbname': db_name})
    dbm = DBManager(params)
    companies = dbm.get_companies_and_vacancies_count()
    print(companies)

    vacancies = dbm.get_all_vacancies()
    print(vacancies)

    avg_salary = dbm.get_avg_salary()
    print(avg_salary)

    vacancies = dbm.get_vacancies_with_higher_salary()
    print(vacancies)

    vacancies = dbm.get_vacancies_with_keyword('специалист')
    print(vacancies)


if __name__ == '__main__':
    main()
