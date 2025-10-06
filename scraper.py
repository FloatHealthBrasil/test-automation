import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import os
from dotenv import load_dotenv
from datetime import date, datetime
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

BASE_URL = os.getenv('ICLIPS_BASE_URL')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
ICLIPS_LOGIN = f"{BASE_URL}/Login"

ENDPOINTS = [
    {
        "path": "/Job/GerarExcel",
        "params": {},
        "payload": {}
    },
    {
        "path": "/Proposta/GerarListaExcel",
        "params": {},
        "payload": {
            "grid":{"gridControlArray":[{"Id":"txt-busca","Value":""},{"Id":"txt-busca-label","Value":"Palavra-chave"},{"Id":"IdJobInt","Value":""},{"Id":"NomeClienteStr","Value":""},{"Id":"IdFuncionario","Value":""},{"Id":"BordereauSaacStr","Value":""},{"Id":"BordereauSaacStr-label","Value":"Saac"},{"Id":"BordereauProcessoStr","Value":""},{"Id":"BordereauProcessoStr-label","Value":"Processo"},{"Id":"BordereauEmpenhoStr","Value":""},{"Id":"BordereauEmpenhoStr-label","Value":"Empenho"},{"Id":"StatusInt","Value":"2"},{"Id":"StatusInt-text","Value":"Aguard. Aprovação"},{"Id":"ExibirValoresInt","Value":"1"},{"Id":"ExibirValoresInt-text","Value":"Cliente"},{"Id":"ValorTotalDec","Value":""},{"Id":"ValorTotalDec-label","Value":"Valor Cobrado"},{"Id":"data-de","Value":""},{"Id":"data-de-label","Value":"Data de Alteração"},{"Id":"data-ate","Value":""},{"Id":"data-ate-label","Value":"Até"},{"Id":"TodosOuSomenteMeus","Value":"0"},{"Id":"TodosOuSomenteMeus-text","Value":"Todos"},{"Id":"txt-busca-simples","Value":""},{"Id":"txt-busca-simples-label","Value":"Busca por palavra-chave"}],"ordemPropriedade":"","ordemDirecaoStr":""}
        }
    },
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
                "Value": ""
            },
            {
                "Id": "filtro-periodo2-inicio-label",
                "Value": "A Partir De"
            },
            {
                "Id": "filtro-periodo2-fim",
                "Value": ""
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
                "Value": "-341"
            },
            {
                "Id": "IdCategoriaLancamento-label",
                "Value": "Subcategoria"
            },
            {
                "Id": "IdCategoriaLancamento-text",
                "Value": "300. Receitas Operacionais"
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
                "Value": "-1"
            },
            {
                "Id": "possui-nota-label",
                "Value": "possui-nota"
            },
            {
                "Id": "possui-nota-text",
                "Value": "Mostrar todos os lançamentos"
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
                "Value": ""
            },
            {
                "Id": "filtro-periodo-inicio-label",
                "Value": "A Partir De"
            },
            {
                "Id": "filtro-periodo-fim",
                "Value": ""
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
    },
    {
        "path": "/PreFaturamento/GerarListaExcel",
        "params": {},
        "payload": {"grid":{"gridControlArray":[{"Id":"txt-busca","Value":""},{"Id":"txt-busca-label","Value":"Palavra-chave"},{"Id":"IdJob","Value":""},{"Id":"IdClienteGrupoStr","Value":""},{"Id":"IdFornecedor","Value":""},{"Id":"IdEmpresa","Value":""},{"Id":"IdProposta","Value":""},{"Id":"TipoInt","Value":""},{"Id":"txt-busca-simples","Value":""},{"Id":"txt-busca-simples-label","Value":"Busca por palavra-chave"}],"ordemPropriedade":"","ordemDirecaoStr":""}}
    }
]

def run_scraper():
    if not BASE_URL or not USER or not PASSWORD:
        raise Exception("Faltam variáveis de ambiente obrigatórias!")
    
    # Configure session with retries and timeouts to be mais resiliente a falhas de rede
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["HEAD", "GET", "POST"]) 
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

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

    response = session.post(ICLIPS_LOGIN, data=login_data, timeout=30)
    response.raise_for_status()

    for i, endpoint in enumerate(ENDPOINTS):
        url = BASE_URL + endpoint["path"]
        params = endpoint.get("params", {})
        payload = endpoint.get("payload", {})


        response = session.post(url, params=params, json=payload, timeout=30)
        response.raise_for_status()

        data = response.json()
        excel_url = data.get('Retorno')
        if not excel_url:
            raise Exception("Excel URL não encontrado na resposta!")

        # normaliza excel_url (pode vir relativo)
        if not excel_url.lower().startswith("http"):
            excel_url = BASE_URL.rstrip('/') + '/' + excel_url.lstrip('/')

        time.sleep(1)
        try:
            excel_response = session.get(excel_url, timeout=30)
            excel_response.raise_for_status()
        except Exception as e:
            # captura e re-levanta com contexto
            raise Exception(f"Falha ao baixar o arquivo Excel em {excel_url}: {e}")

        csv_file = io.BytesIO(excel_response.content)
        
        # Tratativa especial para PreFaturamento
        if endpoint["path"] == "/PreFaturamento/GerarListaExcel":
            try:
                csv_file.seek(0)
                content = csv_file.getvalue()

                # Detecta se o conteúdo é HTML (exportado como tabela) ou um arquivo xlsx
                if b'<table' in content.lower():
                    html = content.decode('utf-8', errors='ignore')
                    soup = BeautifulSoup(html, 'html.parser')
                    table_elements = soup.find_all('table')

                    # Localiza índices dos marcadores 'Custo Interno' e 'Mídia'
                    start_table_idx = None
                    end_table_idx = None
                    for idx, tbl in enumerate(table_elements):
                        txt = tbl.get_text(separator=' ').replace('\xa0', ' ').strip().lower()
                        if 'custo interno' in txt:
                            start_table_idx = idx
                        if 'mídia' in txt:
                            end_table_idx = idx
                            if start_table_idx is not None:
                                break

                    # Converte todas as tabelas possíveis em DataFrame para análise
                    df_converted = []  # lista de tuples (idx, table_element, df)
                    for idx_tbl, tbl in enumerate(table_elements):
                        try:
                            df_try = pd.read_html(io.StringIO(str(tbl)), header=None)[0]
                            df_converted.append((idx_tbl, tbl, df_try))
                        except Exception:
                            continue

                    chosen_df = None
                    # Se encontramos o índice de "Custo Interno", priorizamos a tabela imediatamente após esse marcador
                    if start_table_idx is not None:
                        # procura a primeira tabela convertida com índice > start_table_idx que contenha 'proposta' no cabeçalho ou nas primeiras linhas
                        candidates = [t for t in df_converted if t[0] > start_table_idx and (end_table_idx is None or t[0] < end_table_idx)]
                        for _, tbl_el, df_try in candidates:
                            cols = [str(c).lower() for c in df_try.columns]
                            if any('proposta' in c for c in cols):
                                chosen_df = df_try
                                break
                            maxr = min(3, df_try.shape[0])
                            found = False
                            for r in range(maxr):
                                row_vals = df_try.iloc[r].astype(str).str.lower().tolist()
                                if any('proposta' in v for v in row_vals):
                                    chosen_df = df_try
                                    found = True
                                    break
                            if found:
                                break

                        # se nenhum candidato com 'proposta' foi encontrado, escolhe a primeira tabela após o marcador
                        if chosen_df is None and candidates:
                            chosen_df = candidates[0][2]

                    # fallback: procura em todas as tabelas convertidas por 'proposta'
                    if chosen_df is None:
                        for _, _, df_try in df_converted:
                            cols = [str(c).lower() for c in df_try.columns]
                            if any('proposta' in c for c in cols):
                                chosen_df = df_try
                                break
                            maxr = min(3, df_try.shape[0])
                            found = False
                            for r in range(maxr):
                                row_vals = df_try.iloc[r].astype(str).str.lower().tolist()
                                if any('proposta' in v for v in row_vals):
                                    chosen_df = df_try
                                    found = True
                                    break
                            if found:
                                break

                    if chosen_df is None:
                        raise Exception("Não foi possível localizar a(s) tabela(s) entre 'Custo Interno' e 'Mídia' nem tabela com 'Proposta'.")

                    df = chosen_df

                    # Verifica se o pandas já detectou o header corretamente
                    # Olhando se as COLUNAS já têm os nomes esperados
                    cols_lower = [str(c).lower() for c in df.columns]
                    header_already_set = any('proposta' in c for c in cols_lower)
                    
                    if not header_already_set:
                        # Só procura header nas linhas se ainda não foi detectado pelo pandas
                        header_row = None
                        max_check_rows = min(6, df.shape[0])
                        header_keywords = ['proposta', 'custo interno', 'projeto', 'cliente']
                        for r in range(max_check_rows):
                            row_vals = df.iloc[r].astype(str).str.lower().tolist()
                            if any(any(k in v for k in header_keywords) for v in row_vals):
                                header_row = r
                                break

                        if header_row is not None:
                            header = df.iloc[header_row].astype(str).apply(lambda x: x.strip())
                            df = df[header_row + 1 :].reset_index(drop=True)
                            df.columns = header
                        else:
                            # fallback: se a primeira linha aparenta ser header (contém texto em vez de números), usa-a
                            first_row = df.iloc[0].astype(str).tolist()
                            # considera header se pelo menos metade das células na primeira linha não forem numéricas
                            non_numeric = sum(1 for v in first_row if not v.replace('.', '', 1).replace(',', '', 1).isdigit())
                            if non_numeric >= max(1, len(first_row)//2):
                                header = df.iloc[0].astype(str).apply(lambda x: x.strip())
                                df = df[1:].reset_index(drop=True)
                                df.columns = header
                            else:
                                # não encontrou header explícito; mantém colunas existentes e garante nomes strings
                                df.columns = [str(c).strip() for c in df.columns]

                    # Garante que todas as colunas sejam strings e sem valores vazios
                    df.columns = [str(c).strip() if str(c).strip() != '' else f'col_{i}' for i, c in enumerate(df.columns)]

                    # Se for PreFaturamento e não foi detectado header com 'Proposta', força header esperado
                    if endpoint["path"] == "/PreFaturamento/GerarListaExcel":
                        cols_lower = [str(c).lower() for c in df.columns]
                        if not any('proposta' in c for c in cols_lower):
                            expected = ['Proposta', 'Custo Interno', 'Projeto', 'Título Projeto', 'Cliente', 'Valor', 'Condição de Pagamento', 'Agência', 'Aprovação']
                            if df.shape[1] >= len(expected):
                                df.columns = expected + [f'extra_{i}' for i in range(df.shape[1] - len(expected))]
                            else:
                                df.columns = expected[:df.shape[1]]
                else:
                    # Conteúdo binário; tenta ler como Excel normalmente
                    try:
                        csv_file.seek(0)
                        df = pd.read_excel(csv_file, engine='openpyxl')
                    except Exception:
                        # fallback: tenta detectar HTML preservando acentos
                        csv_file.seek(0)
                        raw = csv_file.read()
                        if b'<table' in raw.lower():
                            decoded = None
                            for enc in ('utf-8', 'cp1252', 'latin-1'):
                                try:
                                    decoded = raw.decode(enc)
                                    break
                                except UnicodeDecodeError:
                                    continue
                            if decoded is None:
                                decoded = raw.decode('latin-1', errors='replace')
                            df = pd.read_html(io.StringIO(decoded), header=None)[0]
                        else:
                            raise Exception('Arquivo não é Excel suportado nem HTML com <table>.')

            except Exception as e:
                raise Exception(f"Erro ao processar o arquivo de Pré-Faturamento: {e}")
        else:
            # Processamento padrão para outros endpoints
            try:
                csv_file.seek(0)
                df = pd.read_excel(csv_file, engine='openpyxl')
            except Exception:
                # fallback: tenta HTML preservando acentos
                csv_file.seek(0)
                raw = csv_file.read()
                if b'<table' in raw.lower():
                    decoded = None
                    for enc in ('utf-8', 'cp1252', 'latin-1'):
                        try:
                            decoded = raw.decode(enc)
                            break
                        except UnicodeDecodeError:
                            continue
                    if decoded is None:
                        decoded = raw.decode('latin-1', errors='replace')
                    tables = pd.read_html(io.StringIO(decoded))
                    if not tables:
                        raise Exception('Nenhuma tabela encontrada no HTML de fallback.')
                    df = tables[0]
                else:
                    raise Exception('Falha ao ler arquivo: não é XLSX suportado e não contém <table>.')

        file_path = f"iclips_data_{i}.csv"
        # Garante que exista um header legível antes de escrever
        import re
        cols = [str(c).strip() for c in df.columns]
        # Se as colunas forem apenas índices numéricos (ex: 0,1,2) ou vazias, tenta extrair header das primeiras linhas
        # IMPORTANTE: Só faz isso se o header ainda não foi processado corretamente
        if all(re.fullmatch(r"\d+", c) for c in cols) or all(c == '' for c in cols):
            found_header = False
            max_check = min(3, df.shape[0])
            for r in range(max_check):
                row = df.iloc[r].astype(str).tolist()
                alpha_count = sum(1 for v in row if re.search(r'[A-Za-zÀ-ÿ]', v))
                if alpha_count >= max(1, len(row) // 2):
                    header = [str(x).strip() for x in row]
                    df = df.drop(df.index[r]).reset_index(drop=True)
                    df.columns = header
                    found_header = True
                    break
            if not found_header:
                df.columns = [f'col_{j}' for j in range(df.shape[1])]
        
        # Verifica se a primeira linha de dados é igual ao header (duplicação)
        # Se sim, remove essa primeira linha
        if df.shape[0] > 0:
            first_row = df.iloc[0].astype(str).str.strip().tolist()
            header_list = [str(c).strip() for c in df.columns]
            if first_row == header_list:
                df = df.iloc[1:].reset_index(drop=True)

        # Remove linhas completamente vazias antes de salvar
        df = df.dropna(how='all').reset_index(drop=True)
        
        # Escreve explicitamente header para garantir preservação
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            cols = [str(c) for c in df.columns]
            f.write(','.join(cols) + '\n')
            df.to_csv(f, index=False, header=False)
