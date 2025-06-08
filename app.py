from __future__ import annotations
"""
BY Contador: Victor Ribeiro /
CRC:  PE-034089/O-6
"""

import logging, os
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from langchain.memory import ConversationBufferWindowMemory
from langchain_groq import ChatGroq
from langchain.prompts import ChatPromptTemplate
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib import colors

from db_utils import (
    deletar,
    faturamento_por_descricao,
    inserir,
    listar_transacoes,
    tentar_extrair_comando,
    totais,
)


# Adicione após as importações no app.py
def clear_transactions_cache():
    keys = ["listar_transacoes_cached", "faturamento_por_descricao_cached"]
    for key in keys:
        if key in st.session_state:
            del st.session_state[key]

# ---------- Config -------------------------------------------------
BASE_PATH   = Path(__file__).parent
CLIENT_NAME = "TAB CELL Loja 1"
MODEL_NAME  = "deepseek-r1-distill-llama-70b"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(BASE_PATH / "app.log")
    ],
)

load_dotenv()
API_KEY = os.getenv("GROQ_API_KEY")
if not API_KEY:
    st.error("GROQ_API_KEY não definida!")
    st.stop()

# ---------- Cached helpers (performance) ---------------------------
@st.cache_data(ttl=300)
def listar_transacoes_cached(
    data_inicio: str | None = None, 
    data_fim: str | None = None
) -> pd.DataFrame:
    return listar_transacoes(data_inicio, data_fim)

@st.cache_data(ttl=300)
def faturamento_por_descricao_cached(
    data_inicio: str | None = None, 
    data_fim: str | None = None
) -> pd.DataFrame:
    return faturamento_por_descricao(data_inicio, data_fim)

# ---------- Funções utilitárias otimizadas ------------------------
def get_totais(
    data_inicio: str | None = None, 
    data_fim: str | None = None
) -> tuple[float, float, float]:
    t = totais(data_inicio, data_fim)
    return t.get("faturamento", 0.0), t.get("despesa", 0.0), t.get("faturamento", 0.0) - t.get("despesa", 0.0)

def gerar_pdf(df: pd.DataFrame, data_inicio: str | None = None, data_fim: str | None = None) -> BytesIO:
    """Gera relatório PDF com tabela formatada e período no título."""
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter)
    
    data = [["ID", "Data", "Tipo", "Valor (R$)", "Descrição"]]
    for _, r in df.iterrows():
        data.append([
            str(int(r.id)),
            r.data.strftime('%d/%m/%Y'),
            r.tipo.capitalize(),
            f"{r.valor:,.2f}",
            r.descricao or ''
        ])
    
    table = Table(data)
    style = TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#4b6cb7")),
        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,0), 10),
        ('BOTTOMPADDING', (0,0), (-1,0), 12),
        ('BACKGROUND', (0,1), (-1,-1), colors.HexColor("#f0f2f6")),
        ('GRID', (0,0), (-1,-1), 1, colors.black)
    ])
    table.setStyle(style)
    
    elements = []
    
    titulo = f"Relatório Financeiro - {CLIENT_NAME}"
    if data_inicio or data_fim:
        periodo = "Período: "
        if data_inicio:
            periodo += f"{datetime.fromisoformat(data_inicio).strftime('%d/%m/%Y')}"
        else:
            periodo += "início"
        if data_fim:
            periodo += f" a {datetime.fromisoformat(data_fim).strftime('%d/%m/%Y')}"
        else:
            periodo += " a hoje"
        titulo += f" ({periodo})"
    
    elements.append(Table([[titulo]], style=[
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTSIZE', (0,0), (-1,-1), 14),
        ('BOTTOMPADDING', (0,0), (-1,-1), 12),
    ]))
    elements.append(Table([[f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}"]], style=[
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTSIZE', (0,0), (-1,-1), 10),
        ('BOTTOMPADDING', (0,0), (-1,-1), 20),
    ]))
    elements.append(table)
    
    df_fat = df[df['tipo']=='faturamento']
    if not df_fat.empty:
        total_fat = df_fat['valor'].sum()
        resumo = df_fat.groupby('descricao')['valor'].sum().reset_index()
        resumo = resumo.sort_values('valor', ascending=False)
        resumo['descricao'] = resumo['descricao'].fillna('(sem descrição)')
        
        elements.append(Table([[""]], style=[('SPACEAFTER', (0,0), (-1,-1), 20)]))
        elements.append(Table([["Resumo de Faturamento por Descrição"]], style=[
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#2c7744")),
            ('TEXTCOLOR', (0,0), (-1,-1), colors.whitesmoke),
            ('FONTSIZE', (0,0), (-1,-1), 12),
            ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ]))
        resumo_data = [["Descrição", "Total (R$)"]]
        for _, r in resumo.iterrows():
            resumo_data.append([r['descricao'], f"{r['valor']:,.2f}"])
        resumo_data.append(["**TOTAL**", f"**{total_fat:,.2f}**"])
        resumo_table = Table(resumo_data)
        resumo_style = TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#5a9c76")),
            ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('BACKGROUND', (0,1), (-1,-2), colors.HexColor("#e8f5e9")),
            ('BACKGROUND', (0,-1), (-1,-1), colors.HexColor("#ffcc80")),
            ('FONTNAME', (0,-1), (-1,-1), 'Helvetica-Bold'),
            ('GRID', (0,0), (-1,-1), 1, colors.black)
        ])
        resumo_table.setStyle(resumo_style)
        elements.append(resumo_table)
    
    doc.build(elements)
    buf.seek(0)
    return buf

# ---------- LLM ----------------------------------------------------
client = ChatGroq(api_key=API_KEY, model=MODEL_NAME)
MEM_DEFAULT = ConversationBufferWindowMemory(k=5, return_messages=True)

def make_prompt() -> ChatPromptTemplate:
    fat, dep, saldo = get_totais()
    sys = (
        f"Você é Victor, assistente virtual da TAB CELL, empresa voltada a manutenção "
        f"e reparação de smartphones \"{CLIENT_NAME}\".\n"
        f"Resumo financeiro {datetime.now():%d/%m/%Y}: "
        f"Faturamento R$ {fat:,.2f}, Despesas R$ {dep:,.2f}, Saldo R$ {saldo:,.2f}."
    )
    anti = "Só utilize fatos válidos do resumo financeiro acima; se não souber, diga 'não sei'."
    return ChatPromptTemplate.from_messages([
        ("system", sys),
        ("system", anti),
        ("placeholder", "{chat_history}"),
        ("user", "{input}")
    ])

# ---------- Persistência via chat ----------------------------------
def registrar_if_needed(msg: str) -> bool:
    reg = tentar_extrair_comando(msg)
    if not reg:
        return False
    inserir(reg)
    st.cache_data.clear()
    st.session_state["toast"] = (
        f"✅ {reg['tipo'].capitalize()} de R$ {reg['valor']:,.2f} salvo!"
        + (f" — {reg['descricao']}" if reg['descricao'] else "")
    )
    st.rerun()
    return True

# ---------- Paginação de resultados -------------------------------
def paginated_transactions(df: pd.DataFrame, page_size: int = 10) -> tuple[pd.DataFrame, int]:
    if df.empty:
        return df, 0
    
    total_pages = (len(df) + page_size - 1) // page_size
    page = st.session_state.get("current_page", 1)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("◀ Anterior", disabled=(page <= 1), use_container_width=True):
            st.session_state["current_page"] = max(1, page - 1)
            st.rerun()
    with col2:
        st.markdown(f"**Página {page} de {total_pages}**", unsafe_allow_html=True)
    with col3:
        if st.button("Próximo ▶", disabled=(page >= total_pages), use_container_width=True):
            st.session_state["current_page"] = min(total_pages, page + 1)
            st.rerun()
    
    start_idx = (page - 1) * page_size
    return df.iloc[start_idx:start_idx + page_size], total_pages

# ---------- Interface (sidebar) ------------------------------------
def sidebar():
    with st.sidebar:
        tabs = st.tabs(["Chat", "Financeiro", "Config"])

        # Chat
        with tabs[0]:
            if st.button("🗑️ Limpar histórico", use_container_width=True):
                st.session_state["memoria"] = ConversationBufferWindowMemory(
                    k=5, return_messages=True
                )
                st.toast("Histórico limpo!")

        # Financeiro
        with tabs[1]:
            st.subheader("Filtrar por Período")
            col1, col2 = st.columns(2)
            with col1:
                data_inicio = st.date_input(
                    "Data Inicial",
                    value=None,
                    key="filtro_data_inicio"
                )
            with col2:
                data_fim = st.date_input(
                    "Data Final",
                    value=None,
                    key="filtro_data_fim"
                )
            
            data_inicio_str = data_inicio.isoformat() if data_inicio else None
            data_fim_str = data_fim.isoformat() if data_fim else None
            
            if st.button("🧹 Limpar Filtros", use_container_width=True):
                # Reseta APENAS os filtros de data
                 st.session_state.filtro_data_inicio = None
                 st.session_state.filtro_data_fim = None
                 st.rerun()  # Recarrega a página com filtros resetados
            
            # PRINCIPAL ALTERAÇÃO: Usar datas do filtro para calcular totais
            fat, dep, saldo = get_totais(data_inicio_str, data_fim_str)
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Faturamento", f"R$ {fat:,.2f}")
            col2.metric("Despesas",   f"R$ {dep:,.2f}")
            col3.metric("Saldo",      f"R$ {saldo:,.2f}")
            
            df_full = listar_transacoes_cached(data_inicio_str, data_fim_str)
            
            if df_full.empty:
                st.info("Sem lançamentos.")
            else:
                page_size = st.select_slider(
                    "Itens por página", 
                    options=[5, 10, 20, 50], 
                    value=10,
                    key="page_size"
                )
                
                df_page, total_pages = paginated_transactions(df_full, page_size)
                
                st.markdown("### Lançamentos")
                for _, r in df_page.iterrows():
                    with st.container(border=True):
                        col1, col2 = st.columns([1, 4])
                        col1.markdown(f"**ID {int(r.id)}**")
                        col2.markdown(f"**{r.data.strftime('%d/%m/%Y')}**")
                        
                        col1, col2, col3 = st.columns(3)
                        col1.markdown(f"**Tipo:** {r.tipo.capitalize()}")
                        col2.markdown(f"**Valor:** R$ {r.valor:,.2f}")
                        
                        if col3.button("🗑️ Excluir", key=f"del-{r.id}", use_container_width=True):
                            try:
                                if deletar(int(r.id)):
                                    st.cache_data.clear()
                                    if "current_page" in st.session_state:
                                        del st.session_state["current_page"]
                                    st.success("Lançamento excluído com sucesso!")
                                    st.rerun()
                                else:
                                    st.error("Registro não encontrado ou já excluído")
                            except Exception as e:
                                st.error(f"Erro ao excluir: {str(e)}")
                        
                        if r.descricao:
                            st.markdown(f"**Descrição:** {r.descricao}")

                st.markdown("### Faturamento por descrição")
                
                df_faturamento = faturamento_por_descricao_cached(data_inicio_str, data_fim_str)
                if not df_faturamento.empty:
                    st.dataframe(
                        df_faturamento.head(10).rename(
                            columns={"descricao": "Descrição", "total": "Total (R$)"}
                        ),
                        hide_index=True,
                        height=250,
                    )
                    
                    if len(df_faturamento) > 10 and st.button("Ver todos os resultados", use_container_width=True):
                        st.dataframe(
                            df_faturamento.rename(
                                columns={"descricao": "Descrição", "total": "Total (R$)"}
                            ),
                            hide_index=True,
                            height=500,
                        )
                else:
                    st.info("Sem dados de faturamento")

                if st.button("📊 Gerar relatório completo em PDF", use_container_width=True):
                    with st.spinner("Gerando relatório... (pode levar alguns segundos)"):
                        pdf = gerar_pdf(df_full, data_inicio_str, data_fim_str)
                        st.download_button(
                            "⬇️ Baixar relatório em PDF",
                            data=pdf,
                            file_name="relatorio_financeiro.pdf",
                            mime="application/pdf",
                            use_container_width=True
                        )

        # Config
        with tabs[2]:
            st.markdown(f"**Modelo LLM:** {MODEL_NAME}")
            if st.button("🔄 Limpar cache de dados", use_container_width=True):
                st.cache_data.clear()
                st.toast("Cache limpo!")

# ---------- Página principal do chat -------------------------------
def chat_page():
    st.header("🤖 Analista Financeiro")

    mem = st.session_state.get("memoria", MEM_DEFAULT)
    for m in mem.buffer_as_messages:
        st.chat_message(m.type).markdown(m.content)

    entrada = st.chat_input("Digite aqui…")
    if not entrada:
        return
    st.chat_message("human").markdown(entrada)

    lower = entrada.strip().lower()

    if lower in {"faturamento por descricao", "faturamento por descrição"}:
        df_desc = faturamento_por_descricao_cached()
        if df_desc.empty:
            st.chat_message("ai").markdown("Ainda não há lançamentos de faturamento.")
        else:
            st.chat_message("ai").markdown(
                "\n".join(
                    f"• **{row.descricao or '(sem descrição)'}**: R$ {row.total:,.2f}"
                    for row in df_desc.itertuples()
                )
            )
        return

    if registrar_if_needed(entrada):
        return

    resposta = (
        st.chat_message("ai")
        .write_stream(
            (make_prompt() | client).stream({
                "input": entrada,
                "chat_history": mem.buffer_as_messages,
            })
        )
    )
    mem.chat_memory.add_user_message(entrada)
    mem.chat_memory.add_ai_message(resposta)
    st.session_state["memoria"] = mem

# ---------- Entry-point --------------------------------------------
def main():
    if "memoria" not in st.session_state:
        st.session_state["memoria"] = ConversationBufferWindowMemory(
            k=5, return_messages=True
        )
    
    sidebar()
    if msg := st.session_state.pop("toast", None):
        st.toast(msg)
    chat_page()

if __name__ == "__main__":
    main()