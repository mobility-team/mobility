from mobility-tools.get_survey_data import get_survey_data


def test_parser_emp_2019():
    df = get_survey_data()


def test_parser_entd_2008():
    df = get_survey_data("ENTD-2008")
