"""
VANTA Black - CSS Styles
Premium dark theme styling for the application.
"""

# CSS styles as a string constant
PREMIUM_CSS = """
<style>
    .stApp {
        background-color: #0e1117;
        background-image: 
            radial-gradient(at 0% 0%, rgba(45, 55, 72, 0.6) 0px, transparent 50%),
            radial-gradient(at 100% 0%, rgba(20, 30, 60, 0.6) 0px, transparent 50%),
            radial-gradient(at 100% 100%, rgba(45, 55, 72, 0.6) 0px, transparent 50%),
            radial-gradient(at 0% 100%, rgba(20, 30, 60, 0.6) 0px, transparent 50%);
        background-attachment: fixed;
        color: #E0E0E0;
    }
    
    /* Remove ALL Container Borders (Nuclear Option) */
    [data-testid="stVerticalBlockBorderWrapper"], [data-testid="stVerticalBlockBorderWrapper"] > div {
        border: none !important;
        box-shadow: none !important;
        background: transparent !important;
    }

    .tf-badge {
        background: linear-gradient(135deg, #ECEFF1, #B0BEC5);
        color: #263238; padding: 3px 10px; border-radius: 12px;
        font-size: 0.85em; font-weight: 700; margin-left: 8px;
        border: 1px solid rgba(255,255,255,0.4);
        box-shadow: 0 0 10px rgba(176, 190, 197, 0.3);
    }
    /* Clean Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        border-bottom: 0px solid transparent !important;
    }
    .stTabs [data-baseweb="tab"] {
        height: 40px;
        white-space: pre-wrap;
        background-color: transparent !important;
        border: none !important;
        color: #E0E0E0;
    }
    .stTabs [aria-selected="true"] {
         background-color: transparent !important;
         border-bottom: 2px solid #FAFAFA !important;
         color: #FFFFFF !important;
    }
    /* Remove the default grey line */
    .stTabs [data-baseweb="tab-border"] {
         display: none !important;
    }
    /* Remove Code Block frames */
    [data-testid="stCodeBlock"] {
        border: none !important;
        box-shadow: none !important;
    }
    [data-testid="stCodeBlock"] > div {
         border: none !important;
         background-color: transparent !important;
    }
    /* Clean Tab Panel */
    [data-baseweb="tab-panel"] {
         padding-top: 10px !important;
    }
    /* Make header transparent */
    header[data-testid="stHeader"] {
        background: transparent !important;
    }

    /* --- NAVIGATION TABS (Radio) --- */
    [data-testid="stRadio"] > div {
        flex-direction: row;
        gap: 20px; /* Space between textual tabs */
        background: transparent !important;
        padding: 0px;
        display: inline-flex;
        border-bottom: 0px solid rgba(255,255,255,0.1);
    }
    [data-testid="stRadio"] label {
        background: transparent !important;
        padding: 5px 0px; /* Minimal padding */
        color: #90A4AE; /* Muted text */
        font-weight: 500;
        transition: all 0.2s;
        margin-right: 0 !important;
        border: none;
        cursor: pointer;
        border-radius: 0px;
        border-bottom: 2px solid transparent; /* Prepare for underline */
    }
    /* Selected State */
    [data-testid="stRadio"] label[data-checked="true"] {
         color: #FFFFFF !important;
         font-weight: 600;
         border-bottom: 2px solid #FFFFFF !important; /* Simple underline */
         box-shadow: none !important;
    }
    /* Hover State */
    [data-testid="stRadio"] label:hover {
         color: #FFFFFF;
    }
    
</style>
"""


def apply_styles(st):
    """Apply premium CSS styles to the Streamlit app."""
    st.markdown(PREMIUM_CSS, unsafe_allow_html=True)
