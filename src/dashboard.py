import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import duckdb
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Duel Commander 500 - Farpas de Mana",
    page_icon="🏆",
    layout="wide"
)

with st.container():
    col1, col2 = st.columns(spec=[1, 1], gap='small', vertical_alignment='center')
    with col1:
        banner_path = "src/images/banner.jpeg"
        try:
            st.image(banner_path, width=500)
        except FileNotFoundError:
            st.error(f"Erro ao carregar o banner. Verifique o caminho: {banner_path}")
    with col2:
        st.markdown("""
        ### Ferramenta idealizada pelo podcast Farpas de Mana.
        Confira mais sobre o podcast em [Linktree - Farpas de Mana](https://linktr.ee/farpasdemana)
        
        Ferramenta desenvolvida pelo ouvinte [Victor Antoniassi](https://antoniassi.omg.lol/)
        """)

def obter_performance_dos_decks(con):
    logger.info("Iniciando consulta para obter dados de performance dos decks")
    try:
        query = """
        WITH estatisticas_decks AS (
            SELECT 
                LOWER(d.deck) as deck,
                COUNT(*) as total_aparicoes,
                COUNT(*) FILTER (WHERE t.position <= 4) as quantidade_top4,
                ROUND(COUNT(*) FILTER (WHERE t.position <= 4)::FLOAT / COUNT(*)::FLOAT * 100, 2) as taxa_top4,
                MIN(t.date) as primeira_aparicao,
                MAX(t.date) as ultima_aparicao
            FROM tournaments_fact t
            JOIN decks_dim d ON t.deck_id = d.deck_id
            GROUP BY d.deck
            HAVING COUNT(*) >= 2
        )
        SELECT 
            deck,
            total_aparicoes,
            quantidade_top4,
            taxa_top4,
            primeira_aparicao,
            ultima_aparicao,
            CASE 
                WHEN taxa_top4 >= 50 AND total_aparicoes >= (SELECT AVG(total_aparicoes) FROM estatisticas_decks) 
                    THEN 'TOPPERS'
                WHEN taxa_top4 >= 50 
                    THEN 'HIDDEN GEMS'
                WHEN total_aparicoes >= (SELECT AVG(total_aparicoes) FROM estatisticas_decks) 
                    THEN 'QUERIDINHOS DOS FÃS'
                ELSE 'CRINGES'
            END as categoria_performance
        FROM estatisticas_decks
        ORDER BY total_aparicoes DESC, taxa_top4 DESC;
        """
        
        df = con.execute(query).df()
        logger.info(f"Consulta executada com sucesso. Colunas obtidas: {df.columns.tolist()}")
        
        df.columns = df.columns.str.lower()
        logger.info(f"Colunas após conversão: {df.columns.tolist()}")
        
        return df
    except Exception as e:
        logger.error(f"Erro ao executar consulta: {str(e)}")
        raise

def criar_grafico_dispersao(df):
    logger.info("Criando gráfico de dispersão")
    try:
        if df.empty:
            logger.warning("O DataFrame está vazio!")
            return None
            
        colunas_necessarias = ['deck', 'total_aparicoes', 'taxa_top4', 'categoria_performance', 'quantidade_top4']
        for coluna in colunas_necessarias:
            if coluna not in df.columns:
                logger.error(f"Coluna {coluna} não encontrada no DataFrame!")
                raise KeyError(f"Coluna {coluna} não encontrada no DataFrame!")
        
        fig = go.Figure()

        categorias = {
            'TOPPERS': 'rgb(255, 0, 0)',
            'HIDDEN GEMS': 'rgb(0, 255, 0)',
            'QUERIDINHOS DOS FÃS': 'rgb(0, 0, 255)',
            'CRINGES': 'rgb(128, 128, 128)'
        }

        for categoria, cor in categorias.items():
            mascara = df['categoria_performance'] == categoria
            df_categoria = df[mascara]
            
            fig.add_trace(go.Scatter(
                x=df_categoria['total_aparicoes'],
                y=df_categoria['taxa_top4'],
                mode='markers',
                name=categoria,
                marker=dict(
                    size=df_categoria['total_aparicoes'] * 3,
                    color=cor,
                    opacity=0.6,
                    line=dict(width=1, color='DarkSlateGrey')
                ),
                text=df_categoria['deck'],
                hovertemplate="<b>%{text}</b><br>" +
                              "Total de Aparições: %{x}<br>" +
                              "Taxa de Top 4: %{y:.2f}%<br>" +
                              "Quantidade de Top 4: " + df_categoria['quantidade_top4'].astype(str) + "<br>" +
                              "<extra></extra>"
            ))

        logger.info("Configurando layout do gráfico")
        fig.update_layout(
            xaxis_title="Total de Aparições",
            yaxis_title="Taxa de Conversão para Top 4 (%)",
            plot_bgcolor='rgb(30, 30, 30)',
            paper_bgcolor='rgb(30, 30, 30)',
            font=dict(color='white'),
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=1.05,
                xanchor="left",
                x=-0.2,
                bgcolor='rgba(0,0,0,0.5)'
            ),
            height=800
        )

        fig.add_hline(y=50, line_dash="dash", line_color="white", opacity=0.5)
        fig.add_vline(x=df['total_aparicoes'].mean(), line_dash="dash", line_color="white", opacity=0.5)
        
        st.write("### Categoria dos decks")
        with st.container():
            col1, col2 = st.columns(spec=[1, 1], gap='small')
            with col1:
                st.markdown("- **TOPPERS**: Alta taxa de Top 4 (≥ 50%) e muitas aparições (acima da média).")
            with col2:
                st.markdown("- **HIDDEN GEMS**: Alta taxa de Top 4 (≥ 50%), mas menos populares.")
        with st.container():
            col1, col2 = st.columns(spec=[1, 1], gap='small')
            with col1:
                st.markdown("- **QUERIDINHOS DOS FÃS**: Populares (acima da média de aparições), mas com baixa taxa de Top 4 (< 50%).")
            with col2:
                st.markdown("- **CRINGES**: Pouco populares e baixa taxa de Top 4.")

        logger.info("Gráfico criado com sucesso")
        return fig
    except Exception as e:
        logger.error(f"Erro ao criar gráfico: {str(e)}")
        raise

def main():
    st.title("🏆 Duel Commander 500 - Top 4 Conversion")
    
    con = None

    try:
        logger.info("Iniciando aplicação")
        
        script_path = Path(__file__).resolve()
        projeto_path = script_path.parent.parent
        db_path = projeto_path / 'data' / 'refined' / 'duelcmd500.duckdb'
        
        logger.info(f"Procurando banco de dados em: {db_path}")
        
        if not db_path.exists():
            logger.error(f"Banco de dados não encontrado em: {db_path}")
            st.error(f"Banco de dados não encontrado em: {db_path}")
            st.info("Verifique se o caminho do banco está correto e se o arquivo foi gerado pelo script ETL.")
            return

        con = duckdb.connect(str(db_path))
        
        df = obter_performance_dos_decks(con)
        if df is not None and not df.empty:
            fig = criar_grafico_dispersao(df)
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
                st.write("")
        else:
            st.warning("Nenhum dado encontrado para exibição.")
    except Exception as e:
        logger.error(f"Erro ao processar os dados: {str(e)}")
        st.error(f"Erro ao processar os dados: {str(e)}")
    finally:
        if con is not None:
            con.close()
            logger.info("Conexão com o banco fechada")

if __name__ == "__main__":
    main()