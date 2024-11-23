import pandas as pd
from datetime import datetime
import logging
from pathlib import Path
import hashlib
import sys
import duckdb

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Definição dos caminhos
BASE_DIR = Path(__file__).parent.parent
RAW_DATA_DIR = BASE_DIR / 'data' / 'raw'
REFINED_DATA_DIR = BASE_DIR / 'data' / 'refined'

def setup_directories():
    """
    Cria as estruturas de diretório necessárias
    """
    try:
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        REFINED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("Diretórios configurados com sucesso")
    except Exception as e:
        logger.error(f"Erro ao criar diretórios: {e}")
        raise

def generate_store_id(store_name: str) -> str:
    """
    Gera um ID único para a loja usando as 3 primeiras letras e um hash curto
    """
    store_name = store_name.strip().lower()
    prefix = store_name[:3]
    hash_suffix = hashlib.md5(store_name.encode()).hexdigest()[:4]
    return f"{prefix}_{hash_suffix}"

def generate_tournament_id(date: datetime, store_id: str) -> str:
    """
    Gera um ID único para o torneio combinando data e loja
    """
    date_str = date.strftime('%Y%m%d')
    return f"t_{date_str}_{store_id}"

def generate_deck_id(decklist_url: str) -> str:
    """
    Gera um ID único para o deck usando hash da URL
    """
    if pd.isna(decklist_url):
        return None
    return f"d_{hashlib.md5(decklist_url.encode()).hexdigest()[:8]}"

def get_formatted_date(date):
    """
    Retorna diferentes formatos de data
    """
    if pd.isna(date):
        return None, None, None, None
    
    try:
        data_br = date.strftime('%d/%m/%Y')
        mes_ano = date.strftime('%m/%Y')
        nome_mes = date.strftime('%B')  # Nome do mês em inglês
        nome_mes_br = {
            'January': 'Janeiro',
            'February': 'Fevereiro',
            'March': 'Março',
            'April': 'Abril',
            'May': 'Maio',
            'June': 'Junho',
            'July': 'Julho',
            'August': 'Agosto',
            'September': 'Setembro',
            'October': 'Outubro',
            'November': 'Novembro',
            'December': 'Dezembro'
        }.get(nome_mes, nome_mes)
        
        dia_semana = date.strftime('%A')  # Dia da semana em inglês
        dia_semana_br = {
            'Monday': 'Segunda-feira',
            'Tuesday': 'Terça-feira',
            'Wednesday': 'Quarta-feira',
            'Thursday': 'Quinta-feira',
            'Friday': 'Sexta-feira',
            'Saturday': 'Sábado',
            'Sunday': 'Domingo'
        }.get(dia_semana, dia_semana)
        
        return data_br, mes_ano, nome_mes_br, dia_semana_br
    except Exception as e:
        logger.error(f"Erro ao formatar data {date}: {e}")
        return None, None, None, None

def clean_tournament_data(df):
    """
    Limpa e prepara os dados do torneio para análise
    """
    logger.info("Iniciando limpeza dos dados")
    
    # 1. Limpeza das datas
    def standardize_date(date_str):
        try:
            if isinstance(date_str, str):
                if '-' in date_str:
                    return pd.to_datetime(date_str)
                else:
                    return pd.to_datetime(date_str, format='%d-%b-%Y')
            return pd.to_datetime(date_str)
        except Exception as e:
            logger.warning(f"Erro ao converter data '{date_str}': {e}")
            return pd.NaT
    
    # Aplica a limpeza das datas
    df['DATE'] = df['DATE'].apply(standardize_date)
    
    # 2. Gera IDs únicos
    df['STORE_ID'] = df['STORE'].apply(generate_store_id)
    df['DECK_ID'] = df['DECKLIST'].apply(generate_deck_id)
    
    # Gera tournament_id após ter store_id
    df['TOURNAMENT_ID'] = df.apply(
        lambda row: generate_tournament_id(row['DATE'], row['STORE_ID']), 
        axis=1
    )
    
    # 3. Limpeza da coluna POSITION
    df['POSITION'] = pd.to_numeric(df['POSITION'], errors='coerce')
    
    # 4. INFO é mantida como está, apenas sendo tratada para NaN quando vazia
    df['INFO'] = df['INFO'].replace('', pd.NA)
    
    # 5. Criação de dimensões temporais úteis com formato brasileiro
    df['DATA_BR'], df['MES_ANO'], df['NOME_MES'], df['DIA_SEMANA'] = zip(*df['DATE'].apply(get_formatted_date))
    
    # 6. Remove linhas com dados críticos faltantes
    before_drop = len(df)
    df = df.dropna(subset=['DATE', 'POSITION', 'DECK', 'STORE'])
    after_drop = len(df)
    logger.info(f"Registros removidos por dados faltantes: {before_drop - after_drop}")
    
    return df.reset_index(drop=True)

def create_dimensional_tables(df_clean):
    """
    Cria as tabelas dimensionais e de fatos
    """
    logger.info("Criando tabelas dimensionais")
    
    # 1. Tabela de Fatos (Torneios)
    tournaments_fact = df_clean[[
        'TOURNAMENT_ID',
        'DATE',
        'STORE_ID',
        'DECK_ID',
        'POSITION',
        'INFO'
    ]].copy()
    
    # 2. Tabela de Dimensão (Decks)
    decks_dim = df_clean[[
        'DECK_ID',
        'DECK',
        'DECKLIST'
    ]].drop_duplicates().copy()
    decks_dim = decks_dim.dropna(subset=['DECK_ID'])
    
    # 3. Tabela de Dimensão (Lojas)
    stores_dim = df_clean[[
        'STORE_ID',
        'STORE'
    ]].drop_duplicates().copy()
    
    # 4. Tabela de Dimensão (Datas)
    dates_dim = df_clean[[
        'DATE',
        'DATA_BR',
        'MES_ANO',
        'NOME_MES',
        'DIA_SEMANA'
    ]].drop_duplicates().copy()
    
    logger.info(f"""Tabelas criadas com as seguintes dimensões:
    - Torneios (fatos): {len(tournaments_fact)} registros
    - Decks: {len(decks_dim)} registros únicos
    - Lojas: {len(stores_dim)} registros únicos
    - Datas: {len(dates_dim)} registros únicos""")
    
    return tournaments_fact, decks_dim, stores_dim, dates_dim

def save_to_duckdb(tables, table_names, db_path):
    """
    Salva as tabelas no DuckDB
    """
    try:
        logger.info(f"Conectando ao DuckDB: {db_path}")
        con = duckdb.connect(str(db_path))
        
        # Cria e popula as tabelas
        for df, table_name in zip(tables, table_names):
            logger.info(f"Criando tabela: {table_name}")
            
            # Remove a tabela se já existir
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Cria a tabela a partir do DataFrame
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            
            # Log do número de registros
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"Tabela {table_name} criada com {count} registros")
        
        # Cria índices para melhor performance
        logger.info("Criando índices...")
        
        # Índices para a tabela de fatos
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_tournaments_tournament_id 
            ON tournaments_fact(tournament_id)
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_tournaments_deck_id 
            ON tournaments_fact(deck_id)
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_tournaments_store_id 
            ON tournaments_fact(store_id)
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_tournaments_date 
            ON tournaments_fact(date)
        """)
        
        # Índices para as dimensões
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_decks_deck_id 
            ON decks_dim(deck_id)
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_stores_store_id 
            ON stores_dim(store_id)
        """)
        
        # Commit e fecha a conexão
        con.commit()
        con.close()
        
        logger.info("Dados salvos com sucesso no DuckDB")
        
    except Exception as e:
        logger.error(f"Erro ao salvar no DuckDB: {e}")
        raise

def save_to_excel(tables, table_names, excel_path):
    """
    Salva todas as tabelas em um único arquivo Excel
    """
    try:
        logger.info(f"Exportando tabelas para Excel: {excel_path}")
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for df, sheet_name in zip(tables, table_names):
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                logger.info(f"Aba '{sheet_name}' criada com {len(df)} registros")
        
        logger.info("Exportação para Excel concluída com sucesso")
        
    except Exception as e:
        logger.error(f"Erro ao salvar no Excel: {e}")
        raise

def validate_raw_data(df):
    """
    Valida os dados brutos antes do processamento
    """
    required_columns = ['DATE', 'POSITION', 'INFO', 'DECK', 'DECKLIST', 'STORE']
    
    # Verifica colunas obrigatórias
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Colunas obrigatórias faltando: {missing_columns}")
    
    # Verifica se há dados
    if df.empty:
        raise ValueError("DataFrame está vazio")
    
    logger.info("Validação dos dados brutos concluída com sucesso")
    return True

def main():
    """Função principal do processo ETL"""
    try:
        logger.info("Iniciando processo de ETL")
        
        # Configuração dos diretórios
        setup_directories()
        
        # Define caminhos dos arquivos
        input_path = RAW_DATA_DIR / 'CMD500_DB.xlsx'
        db_path = REFINED_DATA_DIR / 'duelcmd500.duckdb'
        excel_path = REFINED_DATA_DIR / 'duelcmd500.xlsx'
        
        # Leitura do Excel
        logger.info(f"Lendo arquivo de entrada: {input_path}")
        
        if not input_path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {input_path}")
        
        try:
            df = pd.read_excel(input_path)
            logger.info(f"Dados carregados: {len(df)} registros")
        except Exception as e:
            logger.error(f"Erro ao ler arquivo Excel: {e}")
            raise
        
        # Validação dos dados brutos
        validate_raw_data(df)
        
        # Limpeza dos dados
        df_clean = clean_tournament_data(df)
        
        # Criação das tabelas dimensionais
        tables = create_dimensional_tables(df_clean)
        table_names = ['tournaments_fact', 'decks_dim', 'stores_dim', 'dates_dim']
        
        # Salvamento no DuckDB
        save_to_duckdb(tables, table_names, db_path)
        
        # Salvamento no Excel
        save_to_excel(tables, table_names, excel_path)
        
        # Validação final: tenta uma consulta simples no DuckDB
        con = duckdb.connect(str(db_path))
        for table_name in table_names:
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"Validação: {table_name} contém {count} registros")
        con.close()
        
        logger.info("Processo de ETL concluído com sucesso")
        return 0
        
    except Exception as e:
        logger.error(f"Erro durante o processo de ETL: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())