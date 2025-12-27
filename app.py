import streamlit as st
import sys
import os

# --- This is needed to find the 'src' module ---
# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# -------------------------------------------------

# We will import our backend logic later
# from src.search.query import Searcher, get_important_contacts

def main():
    st.title("AI 이메일 검색 시스템")

    # --- Search Bar ---
    query = st.text_input("검색어를 입력하세요:", "")

    # --- Search Button ---
    if st.button("검색"):
        if query:
            st.write(f"'{query}'(으)로 검색을 실행합니다...")
            # Placeholder for search results
            # results = searcher.search(query)
            # display_results(results)
        else:
            st.warning("검색어를 입력해주세요.")

if __name__ == "__main__":
    main()
