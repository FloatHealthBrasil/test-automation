import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import os
from dotenv import load_dotenv
from datetime import date, datetime

load_dotenv()

BASE_URL = os.getenv('ICLIPS_BASE_URL')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
ICLIPS_LOGIN = f"{BASE_URL}/Login"

ENDPOINTS = [
    
    {
        "path": "/Lancamento/GerarExcel",
        "params": {
            "centroCusto": "False",
            "cnpj": "True",
            "descricao": "True",
            "condicao": "True",
            "vencimento": "True",
            "pagamento": "True",
            "competencia": "True",
            "documento": "True",
            "categoria": "True",
            "relacao": "True",
            "banco": "True"
        },
        "payload": {
            "grid": {
            "gridControlArray": [
                {
                "Id": "select2Caixa",
                "Value": ""
                },
                {
                "Id": "filtro-busca",
                "Value": ""
                },
                {
                "Id": "filtro-busca-label",
                "Value": "Busca avançada"
                },
                {
                "Id": "TipoInt",
                "Value": "1,3"
                },
                {
                "Id": "TipoInt-text",
                "Value": "Entrada, A Receber"
                },
                {
                "Id": "filtro-periodo2-inicio",
                "Value": date(datetime.now().year, datetime.now().month, 1).strftime('%d/%m/%Y')
                },
                {
                "Id": "filtro-periodo2-inicio-label",
                "Value": "A Partir De"
                },
                {
                "Id": "filtro-periodo2-fim",
                "Value": datetime.now().date().strftime('%d/%m/%Y')
                },
                {
                "Id": "filtro-periodo2-fim-label",
                "Value": "Até"
                },
                {
                "Id": "filtro-periodoCompetencia-inicio",
                "Value": ""
                },
                {
                "Id": "filtro-periodoCompetencia-inicio-label",
                "Value": "A Partir De"
                },
                {
                "Id": "filtro-periodoCompetencia-fim",
                "Value": ""
                },
                {
                "Id": "filtro-periodoCompetencia-fim-label",
                "Value": "Até"
                },
                {
                "Id": "IdCaixa",
                "Value": ""
                },
                {
                "Id": "IdCaixa-label",
                "Value": "Conta"
                },
                {
                "Id": "IdCategoriaLancamento",
                "Value": ""
                },
                {
                "Id": "IdCategoriaLancamento-label",
                "Value": "Subcategoria"
                },
                {
                "Id": "IdCentroCusto",
                "Value": ""
                },
                {
                "Id": "IdCentroCusto-label",
                "Value": "Centro de Custo"
                },
                {
                "Id": "ValorApartirDec",
                "Value": ""
                },
                {
                "Id": "ValorApartirDec-label",
                "Value": "Valor a Partir"
                },
                {
                "Id": "ValorAteDec",
                "Value": ""
                },
                {
                "Id": "ValorAteDec-label",
                "Value": "Valor Até"
                },
                {
                "Id": "CondicaoInt",
                "Value": ""
                },
                {
                "Id": "filtro-tipodocumento",
                "Value": ""
                },
                {
                "Id": "filtro-tipodocumento-label",
                "Value": "Forma de Pagamento"
                },
                {
                "Id": "filtro-numdocumento",
                "Value": ""
                },
                {
                "Id": "filtro-numdocumento-label",
                "Value": "Número do Documento"
                },
                {
                "Id": "NotaFiscalStr",
                "Value": ""
                },
                {
                "Id": "NotaFiscalStr-label",
                "Value": "Nota Fiscal"
                },
                {
                "Id": "possui-nota",
                "Value": "1"
                },
                {
                "Id": "possui-nota-label",
                "Value": "possui-nota"
                },
                {
                "Id": "possui-nota-text",
                "Value": "Sim"
                },
                {
                "Id": "TipoUsuarioStr",
                "Value": ""
                },
                {
                "Id": "TipoUsuarioStr-label",
                "Value": "Tipo Usuário"
                },
                {
                "Id": "IdDestino",
                "Value": ""
                },
                {
                "Id": "IdDestino-label",
                "Value": "Origem/Destino"
                },
                {
                "Id": "FonteInt",
                "Value": ""
                },
                {
                "Id": "FonteInt-label",
                "Value": "Relação"
                },
                {
                "Id": "IdFonteBuscaStr",
                "Value": ""
                },
                {
                "Id": "IdFonteBuscaStr-label",
                "Value": "Cod. Relação"
                },
                {
                "Id": "RelacaoSelect",
                "Value": ""
                },
                {
                "Id": "RelacaoSelect-label",
                "Value": "Nome"
                },
                {
                "Id": "txt-busca-simples",
                "Value": ""
                },
                {
                "Id": "txt-busca-simples-label",
                "Value": "Busca avançada"
                },
                {
                "Id": "filtro-periodo-inicio",
                "Value": "01/06/2025"
                },
                {
                "Id": "filtro-periodo-inicio-label",
                "Value": "A Partir De"
                },
                {
                "Id": "filtro-periodo-fim",
                "Value": "30/06/2025"
                },
                {
                "Id": "filtro-periodo-fim-label",
                "Value": "Até"
                }
            ],
            "ordemPropriedade": "",
            "ordemDirecaoStr": ""
            }
        }
    }
]

def run_scraper():
    if not BASE_URL or not USER or not PASSWORD:
        raise Exception("Faltam variáveis de ambiente obrigatórias!")
    
    session = requests.Session()

    login_page = session.get(ICLIPS_LOGIN)
    soup = BeautifulSoup(login_page.text, 'html.parser')

    token_input = soup.find('input', {'name': '__RequestVerificationToken'})
    csrf_token = token_input['value'] if token_input else None
    if not csrf_token:
        raise Exception("CSRF token não encontrado!")

    login_data = {
        '__RequestVerificationToken': csrf_token,
        'LoginStr': USER,
        'SenhaStr': PASSWORD,
        'ManterConectado': 'false'
    }

    response = session.post(ICLIPS_LOGIN, data=login_data)
    response.raise_for_status()

    for i, endpoint in enumerate(ENDPOINTS):
        url = BASE_URL + endpoint["path"]
        params = endpoint.get("params", {})
        payload = endpoint.get("payload", {})


        response = session.post(url, params=params, json=payload)
        response.raise_for_status()

        data = response.json()
        excel_url = data.get('Retorno')
        if not excel_url:
            raise Exception("Excel URL não encontrado na resposta!")

        excel_response = session.get(excel_url)
        excel_response.raise_for_status()

        csv_file = io.BytesIO(excel_response.content)
        try:
            df = pd.read_excel(csv_file, engine='openpyxl')
        except Exception as e:
            try:
                csv_file.seek(0)
                df = pd.read_html(csv_file)[0]
            except Exception as e:
                raise Exception(f"Erro ao ler o arquivo Excel: {e}")

        file_path = f"iclips_data_{i}.csv"
        df.to_csv(file_path, encoding="utf-8", index=False)
