from plotly.subplots import make_subplots
import plotly.graph_objects as go


# Create plot to be sent over Telegram

def plot_intraday_chart(df_intraday):
    if df_intraday.empty:
        return go.Figure()

    # Create subplots (3 rows)
    fig_intraday = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.6, 0.2, 0.2],
        subplot_titles=[
            f"{df_intraday.iloc[-1]['Symbol']}, "
            f"Time={df_intraday.iloc[-1]['Time']}"
        ]
    )

    # Candlestick
    fig_intraday.add_trace(go.Candlestick(
        x=df_intraday['Time'],
        open=df_intraday['Open'],
        high=df_intraday['High'],
        low=df_intraday['Low'],
        close=df_intraday['Close'],
        name='OHLC'
    ), row=1, col=1)

    # VWAP
    if 'VWAP' in df_intraday.columns:
        fig_intraday.add_trace(go.Scatter(
            x=df_intraday['Time'],
            y=df_intraday['VWAP'],
            mode='lines',
            line=dict(color='red', width=2),
            name='VWAP'
        ), row=1, col=1)

    # EMA9
    if 'EMA9' in df_intraday.columns:
        fig_intraday.add_trace(go.Scatter(
            x=df_intraday['Time'],
            y=df_intraday['EMA9'],
            mode='lines',
            line=dict(color='purple', width=1),
            name='EMA9'
        ), row=1, col=1)

    # Volume
    fig_intraday.add_trace(go.Bar(
        x=df_intraday['Time'],
        y=df_intraday['Volume'],
        marker_color='blue',
        name='Volume'
    ), row=2, col=1)

    # Relatr
    if 'Relatr' in df_intraday.columns:
        fig_intraday.add_trace(go.Scatter(
            x=df_intraday['Time'],
            y=df_intraday['Relatr'],
            mode='lines',
            line=dict(color='green', width=2),
            name='Relatr'
        ), row=3, col=1)

        # Add horizontal lines
        for y_val in [0, 0.5, -0.5]:
            fig_intraday.add_shape(
                type='line',
                x0=df_intraday['Time'].min(),
                x1=df_intraday['Time'].max(),
                y0=y_val,
                y1=y_val,
                line=dict(color='black', width=1, dash='dash'),
                xref='x3', yref='y3'
            )
                    # --- Annotate the latest value ---
        latest_time = df_intraday['Time'].iloc[-1]
        latest_value = df_intraday['Relatr'].iloc[-1]

        fig_intraday.add_annotation(
            x=latest_time,
            y=latest_value,
            xref='x3', yref='y3',
            text=f"{latest_value:.2f}",  # show value with 2 decimals
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            ax=40,  # shift text position
            ay=0,
            font=dict(color="black", size=15),
            bgcolor="rgba(255,255,255,0.7)"
        )
        # Layout
        fig_intraday.update_layout(
            height=800,
            showlegend=False,
            xaxis3=dict(title='Time'),
            yaxis=dict(title='Price'),
            yaxis2=dict(title='Volume'),
            yaxis3=dict(title='Relatr'),
            xaxis_rangeslider_visible=False,
        )
    
    return fig_intraday