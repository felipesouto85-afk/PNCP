import requests
import pandas as pd
from datetime import datetime
import os
import ast
import time
import json

def safe_literal_eval(val):
    if pd.isna(val) or not isinstance(val, str):
        return {}
    try:
        return ast.literal_eval(val)
    except:
        return {}

def download_pncp_final():
    dtb_file = "g:/Meu Drive/TCM-BA/2ª DCE/Dados/PNCP/RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xls"
    output_file = "g:/Meu Drive/TCM-BA/2ª DCE/Dados/PNCP/CONTRATOS_PNCP_BA_MUNICIPAIS.xlsx"
    
    print("--- FASE 1: Carregando códigos IBGE dos municípios da Bahia ---")
    try:
        df_dtb = pd.read_excel(dtb_file)
        ba_codes = set(df_dtb[df_dtb.iloc[:, 0] == 29].iloc[:, 7].astype(str).tolist())
        print(f"Sucesso: {len(ba_codes)} municípios da Bahia carregados do DTB.")
    except Exception as e:
        print(f"Erro ao ler arquivo DTB: {e}")
        return

    base_url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    
    # --- NOVO: Verificação de atualização incremental ---
    df_base = pd.DataFrame()
    data_inicial = "20260401"  # Data base inicial (caso não exista arquivo)
    
    if os.path.exists(output_file):
        try:
            print(f"Lendo base existente para verificar última atualização...")
            df_base = pd.read_excel(output_file)
            if not df_base.empty and 'dataPublicacaoPncp' in df_base.columns:
                # Converter para datetime para achar a maior data
                df_base['dataPublicacaoPncp'] = pd.to_datetime(df_base['dataPublicacaoPncp'], errors='coerce')
                ultima_data = df_base['dataPublicacaoPncp'].max()
                if pd.notna(ultima_data):
                    data_inicial = ultima_data.strftime("%Y%m%d")
                    print(f"Base carregada. Última data encontrada: {ultima_data}. Atualizando a partir de: {data_inicial}")
                    
            # Converter colunas de IDs para string para evitar conflitos de tipo no merge/drop_duplicates
            cols_to_str = ['numeroControlePNCP', 'unidadeOrgao_codigoIbge']
            for c in cols_to_str:
                if c in df_base.columns:
                    df_base[c] = df_base[c].astype(str)
        except Exception as e:
            print(f"Aviso ao carregar base existente: {e}. Iniciando busca do zero.")
            df_base = pd.DataFrame()

    data_hoje = datetime.now().strftime("%Y%m%d")
    uf = "BA"
    modalidades = list(range(1, 14))
    
    import concurrent.futures
    import threading

    todos_contratos = []
    print(f"\n--- FASE 2: Consultando API PNCP ({data_inicial} até {data_hoje}) em Paralelo ---")
    
    lock = threading.Lock()
    
    def baixar_modalidade(mod):
        pagina = 1
        tamanho_atual = 50
        print(f"Iniciando Modalidade: {mod}")
        
        while True:
            params = {
                "dataInicial": data_inicial,
                "dataFinal": data_hoje,
                "codigoModalidadeContratacao": mod,
                "uf": uf,
                "pagina": pagina,
                "tamanhoPagina": tamanho_atual
            }
            
            sucesso = False
            registros = []
            fim_modalidade = False
            
            for tentativa in range(3):
                try:
                    # Timeout reduzido para falhar mais cedo e tentar de novo, evitando o congelamento longo
                    response = requests.get(base_url, params=params, timeout=20)
                    response.raise_for_status()
                    
                    try:
                        data = response.json()
                    except ValueError:
                        print(f"  [Mod {mod}] Pág {pagina}: Falha (Não retornou JSON válido). Tentativa {tentativa+1}")
                        time.sleep(2)
                        continue
                        
                    registros = data.get('data', []) if isinstance(data, dict) else data
                    sucesso = True
                    
                    if not registros:
                        fim_modalidade = True
                        break
                    
                    count = len(registros)
                    with lock:
                        # Tratar primeiro log
                        if len(todos_contratos) == 0:
                            print("\n--- EXEMPLO DE JSON RECEBIDO ---")
                            print(json.dumps(registros[0], indent=2, ensure_ascii=False))
                            print("--------------------------------\n")
                        todos_contratos.extend(registros)
                        
                    print(f"  [Mod {mod}] Página {pagina}: {count} resultados.")
                    
                    # Se vieram menos registros que o tamanho que a API deveria mandar, é a última aba
                    if count < tamanho_atual:
                        fim_modalidade = True
                        break
                        
                    pagina += 1
                    break
                    
                except requests.exceptions.Timeout:
                    print(f"  [Mod {mod}] Pág {pagina}: Timeout (Demora). Tentativa {tentativa+1}")
                    time.sleep(2)
                except Exception as e:
                    print(f"  [Mod {mod}] Pág {pagina}: Erro {type(e).__name__}. Tentativa {tentativa+1}")
                    time.sleep(2)
            
            # Condição de saída total
            if not sucesso or fim_modalidade:
                break

    # Executa buscas em paralelo, permitindo até 5 conexões simultâneas:
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(baixar_modalidade, modalidades)
    
    if not todos_contratos:
        print("\nNenhum contrato encontrado.")
        return

    print(f"\n--- FASE 3: Tratamento e Normalização (Total bruto: {len(todos_contratos)}) ---")
    
    # Usamos json_normalize para desdobrar automaticamente todos os campos aninhados
    # ex: unidadeOrgao.municipioNome -> unidadeOrgao_municipioNome
    df_final = pd.json_normalize(todos_contratos, sep='_')
    
    print("Colunas geradas após normalização:", [c for c in df_final.columns if '_' in c])
    
    # Filtrar pelos códigos IBGE do DTB
    # O campo agora deve se chamar unidadeOrgao_codigoIbge devido ao json_normalize
    col_ibge = 'unidadeOrgao_codigoIbge'
    if col_ibge in df_final.columns:
        df_final[col_ibge] = df_final[col_ibge].astype(str)
        df_filtrado = df_final[df_final[col_ibge].isin(ba_codes)].copy()
        print(f"Registros filtrados (somente municípios BA): {len(df_filtrado)} de {len(df_final)}")
    else:
        # Tenta nomes alternativos caso o schema mude
        alt_cols = [c for c in df_final.columns if 'codigoIbge' in c]
        if alt_cols:
            col_ibge = alt_cols[0]
            df_final[col_ibge] = df_final[col_ibge].astype(str)
            df_filtrado = df_final[df_final[col_ibge].isin(ba_codes)].copy()
            print(f"Registros filtrados usando '{col_ibge}': {len(df_filtrado)} de {len(df_final)}")
        else:
            print("Aviso: Coluna de código IBGE não encontrada após normalização. Salvando arquivo bruto.")
            df_filtrado = df_final

    # 1. Tratamento da coluna 'fontesOrcamentarias'
    def extract_fontes(val):
        if val is None:
            return "Não declarado"
        
        # Se for lista nativa, evita o pd.isna() que causa ValueError
        if isinstance(val, (list, tuple)):
            if len(val) == 0:
                return "Não declarado"
            descricoes = [str(item.get('descricao', '')) for item in val if isinstance(item, dict) and item.get('descricao')]
            if descricoes:
                return " / ".join(descricoes)
            return "Não declarado"
            
        if isinstance(val, float) and pd.isna(val):
            return "Não declarado"
            
        if isinstance(val, str):
            try:
                import json
                val = json.loads(val.replace("'", '"'))
            except:
                try:
                    import ast
                    val = ast.literal_eval(val)
                except:
                    pass
            
            if isinstance(val, (list, tuple)):
                if len(val) == 0:
                    return "Não declarado"
                descricoes = [str(item.get('descricao', '')) for item in val if isinstance(item, dict) and item.get('descricao')]
                if descricoes:
                    return " / ".join(descricoes)
                    
        return "Não declarado"

    if 'fontesOrcamentarias' in df_filtrado.columns:
        df_filtrado['fontesOrcamentarias'] = df_filtrado['fontesOrcamentarias'].apply(extract_fontes)

    # 2. Tradução de códigos
    map_modalidade = {
        1: 'Leilão - Eletrônico', 2: 'Diálogo Competitivo', 3: 'Concurso', 
        4: 'Concorrência - Eletrônica', 5: 'Concorrência - Presencial', 6: 'Pregão - Eletrônico', 
        7: 'Pregão - Presencial', 8: 'Dispensa de Licitação', 9: 'Inexigibilidade', 
        10: 'Manifestação de Interesse', 11: 'Pré-qualificação', 12: 'Credenciamento', 13: 'Leilão - Presencial'
    }
    map_modo_disputa = {
        1: 'Aberto', 2: 'Fechado', 3: 'Aberto-Fechado', 
        4: 'Dispensa Com Disputa', 5: 'Não se aplica', 6: 'Fechado-Aberto'
    }
    map_criterio = {
        1: 'Menor preço', 2: 'Maior desconto', 4: 'Técnica e preço', 
        5: 'Maior lance', 6: 'Maior retorno econômico', 7: 'Não se aplica', 
        8: 'Melhor técnica', 9: 'Conteúdo artístico'
    }
    map_situacao_compra = {
        1: 'Divulgada no PNCP', 2: 'Revogada', 3: 'Anulada', 4: 'Suspensa'
    }
    map_situacao_item = {
        1: 'Em Andamento', 2: 'Homologado', 3: 'Anulado/Revogado/Cancelado', 
        4: 'Deserto', 5: 'Fracassado'
    }
    map_tipo_contrato = {
        1: 'Contrato (termo inicial)', 2: 'Comodato', 3: 'Arrendamento', 4: 'Concessão', 
        5: 'Termo de Adesão', 6: 'Convênio', 7: 'Empenho', 8: 'Outros', 
        9: 'Termo de Execução Descentralizada (TED)', 10: 'Acordo de Cooperação Técnica (ACT)', 
        11: 'Termo de Compromisso', 12: 'Carta Contrato'
    }

    coluna_mapas = {
        'modalidadeId': map_modalidade,
        'modoDisputaId': map_modo_disputa,
        'criterioJulgamentoId': map_criterio,
        'situacaoCompraId': map_situacao_compra,
        'situacaoItemContratacaoId': map_situacao_item,
        'tipoContratoId': map_tipo_contrato
    }

    for col, mapping in coluna_mapas.items():
        if col in df_filtrado.columns:
            df_filtrado[col] = df_filtrado[col].map(mapping).fillna(df_filtrado[col])

    # 3. Limpeza de caracteres invisíveis/ilegais (impede erro openpyxl.utils.exceptions.IllegalCharacterError)
    try:
        from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
        for col in df_filtrado.select_dtypes(include=['object', 'string']):
            df_filtrado[col] = df_filtrado[col].apply(
                lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x
            )
    except Exception as e:
        print(f"Aviso: Não foi possível limpar os caracteres ilegais: {e}")

    # --- NOVO: Mesclagem com a base antiga ---
    if not df_base.empty:
        print(f"\n--- FASE 4: Consolidando com a base anterior ---")
        # Garantir que os tipos batam em colunas críticas antes de concatenar
        if 'numeroControlePNCP' in df_filtrado.columns:
            df_filtrado['numeroControlePNCP'] = df_filtrado['numeroControlePNCP'].astype(str)
        
        # Concatenar novo com antigo
        df_consolidado = pd.concat([df_base, df_filtrado], ignore_index=True)
        
        # Remover duplicatas baseadas no numeroControlePNCP (identificador único da contratação no PNCP)
        if 'numeroControlePNCP' in df_consolidado.columns:
            total_antes = len(df_consolidado)
            df_consolidado = df_consolidado.drop_duplicates(subset=['numeroControlePNCP'], keep='last')
            total_depois = len(df_consolidado)
            print(f"Registros novos/atualizados processados. Duplicatas removidas: {total_antes - total_depois}")
        
        df_filtrado = df_consolidado

    # Salva o arquivo final atualizado
    df_filtrado.to_excel(output_file, index=False)
    print(f"\nCONCLUÍDO! Total de registros na base agora: {len(df_filtrado)}")
    print(f"Arquivo salvo em: {output_file}")

if __name__ == "__main__":
    download_pncp_final()
