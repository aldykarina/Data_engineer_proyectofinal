from airflow.models import Variable


def generate_email_content_and_subject(**context):
    symbols = ['SP&500', 'Apple', 'Google', 'Bitcoin', 'Ethereum']
    acronyms = ['SPY', 'AAPL', 'GOOGL','BTC/USD', 'ETH/USD']

    texts = [
        f'Symbols {symbols} ({acronyms})'
        for symbols, acronyms, in zip(symbols, acronyms)
    ]

    estimates = '\n'.join(texts)
    context['ti'].xcom_push(key='estimates', value=estimates)

    retry_count = context['ti'].try_number
    base_subject = Variable.get("subject_mail")
    
    if retry_count > 1:
        subject = f"{base_subject} (Retry Attempt {retry_count - 1}))"
    else:
        subject = base_subject + " - Resultado OK "
    
    context['ti'].xcom_push(key='email_subject', value=subject)