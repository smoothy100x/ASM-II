import csv
import json
import math
import re
import struct
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "Collector" / "data"
OUTPUT_DIR = Path(__file__).resolve().parent / "indicators_output"
THREADS = 8
EPSILON = 1e-10
INDICATOR_SPECS = (
    (1, 'Simple Moving Average (SMA)', 14, 'mid-price series'),
    (2, 'Exponential Moving Average (EMA)', 14, 'mid-price series'),
    (3, 'Weighted Moving Average (WMA)', 14, 'mid-price series'),
    (4, 'Wilder Moving Average (RMA)', 14, 'mid-price series'),
    (5, 'Smoothed Moving Average (SMMA)', 14, 'mid-price series'),
    (6, 'Double Exponential Moving Average (DEMA)', 14, 'mid-price series'),
    (7, 'Triple Exponential Moving Average (TEMA)', 14, 'mid-price series'),
    (8, 'Triangular Moving Average (TRIMA)', 14, 'mid-price series'),
    (9, 'Hull Moving Average (HMA)', 14, 'mid-price series'),
    (10, 'Kaufman Adaptive Moving Average (KAMA)', 14, 'mid-price series'),
    (11, 'McGinley Dynamic', 14, 'mid-price series'),
    (12, 'Arnaud Legoux Moving Average (ALMA)', 14, 'mid-price series'),
    (13, 'Zero-Lag Exponential Moving Average (ZLEMA)', 14, 'mid-price series'),
    (14, 'Mesa Adaptive Moving Average (MAMA)', 14, 'mid-price series'),
    (15, 'Tillson T3 Moving Average', 14, 'mid-price series'),
    (16, 'Least Squares Moving Average (LSMA)', 14, 'mid-price series'),
    (17, 'Relative Strength Index (RSI)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (18, 'Stochastic Oscillator', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (19, 'Stochastic RSI', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (20, 'Moving Average Convergence Divergence (MACD)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (21, 'Percentage Price Oscillator (PPO)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (22, 'Absolute Price Oscillator (APO)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (23, 'Rate of Change (ROC)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (24, 'Momentum (MOM)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (25, 'Chande Momentum Oscillator (CMO)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (26, 'True Strength Index (TSI)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (27, 'Williams %R', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (28, 'Commodity Channel Index (CCI)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (29, 'Connors RSI', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (30, 'Know Sure Thing (KST)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (31, 'Elder Ray Index', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (32, 'Awesome Oscillator', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (33, 'Average Directional Index (ADX)', 14, 'resampled mid-price bars unless stated'),
    (34, 'Average Directional Index Rating (ADXR)', 14, 'resampled mid-price bars unless stated'),
    (35, 'Directional Movement Index (DX)', 14, 'resampled mid-price bars unless stated'),
    (36, 'Plus Directional Indicator (+DI)', 14, 'resampled mid-price bars unless stated'),
    (37, 'Minus Directional Indicator (-DI)', 14, 'resampled mid-price bars unless stated'),
    (38, 'Aroon Indicator', 14, 'resampled mid-price bars unless stated'),
    (39, 'Aroon Oscillator', 14, 'resampled mid-price bars unless stated'),
    (40, 'Vortex Indicator', 14, 'resampled mid-price bars unless stated'),
    (41, 'Supertrend', 14, 'resampled mid-price bars unless stated'),
    (42, 'Parabolic SAR', 14, 'resampled mid-price bars unless stated'),
    (43, 'Choppiness Index', 14, 'resampled mid-price bars unless stated'),
    (44, 'Linear Regression Slope', 14, 'resampled mid-price bars unless stated'),
    (45, 'Trend Strength Index', 14, 'resampled mid-price bars unless stated'),
    (46, 'Average True Range (ATR)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (47, 'Normalized Average True Range (NATR)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (48, 'Bollinger Bands', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (49, 'Bollinger %B', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (50, 'Bollinger Band Width', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (51, 'Keltner Channels', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (52, 'Donchian Channels', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (53, 'Average Range', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (54, 'Historical Volatility', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (55, 'Parkinson Volatility', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (56, 'Garman-Klass Volatility', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (57, 'Chaikin Volatility', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (58, 'Ulcer Index', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (59, 'On-Balance Volume (OBV)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (60, 'Volume-Weighted Average Price (VWAP)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (61, 'Volume Weighted Moving Average (VWMA)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (62, 'Money Flow Index (MFI)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (63, 'Chaikin Money Flow (CMF)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (64, 'Chaikin Accumulation/Distribution Line', 14, 'actual trade volume required; unavailable in these CSVs'),
    (65, 'Chaikin A/D Oscillator', 14, 'actual trade volume required; unavailable in these CSVs'),
    (66, 'Volume Price Trend (VPT)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (67, 'Price Volume Trend (PVT)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (68, 'Ease of Movement (EMV)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (69, 'Force Index', 14, 'actual trade volume required; unavailable in these CSVs'),
    (70, 'Klinger Volume Oscillator', 14, 'actual trade volume required; unavailable in these CSVs'),
    (71, 'Best Bid-Ask Spread', 14, 'directly computable from LOB snapshots'),
    (72, 'Relative Bid-Ask Spread', 14, 'directly computable from LOB snapshots'),
    (73, 'Mid-Price', 14, 'directly computable from LOB snapshots'),
    (74, 'Microprice', 14, 'directly computable from LOB snapshots'),
    (75, 'Top-Level Queue Imbalance', 14, 'directly computable from LOB snapshots'),
    (76, 'Level-Wise Queue Imbalance', 14, 'directly computable from LOB snapshots'),
    (77, 'Cumulative Depth Imbalance', 14, 'directly computable from LOB snapshots'),
    (78, 'Multi-Level Order Book Imbalance', 14, 'directly computable from LOB snapshots'),
    (79, 'Bid-Ask Depth Ratio', 14, 'directly computable from LOB snapshots'),
    (80, 'Weighted Mid-Price', 14, 'directly computable from LOB snapshots'),
    (81, 'Volume-Weighted Bid Price', 14, 'directly computable from LOB snapshots'),
    (82, 'Volume-Weighted Ask Price', 14, 'directly computable from LOB snapshots'),
    (83, 'Bid Depth Slope', 14, 'directly computable from LOB snapshots'),
    (84, 'Ask Depth Slope', 14, 'directly computable from LOB snapshots'),
    (85, 'Order Book Depth Concentration', 14, 'directly computable from LOB snapshots'),
    (86, 'Simple Return', 14, 'mid-price series'),
    (87, 'Log Return', 14, 'mid-price series'),
    (88, 'Cumulative Return', 14, 'mid-price series'),
    (89, 'Rolling Mean Return', 14, 'mid-price series'),
    (90, 'Rolling Return Standard Deviation', 14, 'mid-price series'),
    (91, 'Rolling Return Variance', 14, 'mid-price series'),
    (92, 'Rolling Skewness', 14, 'mid-price series'),
    (93, 'Rolling Kurtosis', 14, 'mid-price series'),
    (94, 'Rolling Median Absolute Deviation', 14, 'mid-price series'),
    (95, 'Rolling Quantile Range', 14, 'mid-price series'),
    (96, 'Rolling Autocorrelation', 14, 'mid-price series'),
    (97, 'Rolling Partial Autocorrelation', 14, 'mid-price series'),
    (98, 'Rolling Hurst Exponent', 14, 'mid-price series'),
    (99, 'Rolling Variance Ratio', 14, 'mid-price series'),
    (100, 'Rolling Z-Score', 14, 'mid-price series'),
    (101, 'Simple Moving Average (SMA)', 5, 'mid-price series'),
    (102, 'Simple Moving Average (SMA)', 10, 'mid-price series'),
    (103, 'Simple Moving Average (SMA)', 14, 'mid-price series'),
    (104, 'Simple Moving Average (SMA)', 20, 'mid-price series'),
    (105, 'Exponential Moving Average (EMA)', 5, 'mid-price series'),
    (106, 'Exponential Moving Average (EMA)', 10, 'mid-price series'),
    (107, 'Exponential Moving Average (EMA)', 14, 'mid-price series'),
    (108, 'Exponential Moving Average (EMA)', 20, 'mid-price series'),
    (109, 'Weighted Moving Average (WMA)', 5, 'mid-price series'),
    (110, 'Weighted Moving Average (WMA)', 10, 'mid-price series'),
    (111, 'Weighted Moving Average (WMA)', 14, 'mid-price series'),
    (112, 'Weighted Moving Average (WMA)', 20, 'mid-price series'),
    (113, 'Wilder Moving Average (RMA)', 5, 'mid-price series'),
    (114, 'Wilder Moving Average (RMA)', 10, 'mid-price series'),
    (115, 'Wilder Moving Average (RMA)', 14, 'mid-price series'),
    (116, 'Wilder Moving Average (RMA)', 20, 'mid-price series'),
    (117, 'Smoothed Moving Average (SMMA)', 5, 'mid-price series'),
    (118, 'Smoothed Moving Average (SMMA)', 10, 'mid-price series'),
    (119, 'Smoothed Moving Average (SMMA)', 14, 'mid-price series'),
    (120, 'Smoothed Moving Average (SMMA)', 20, 'mid-price series'),
    (121, 'Double Exponential Moving Average (DEMA)', 5, 'mid-price series'),
    (122, 'Double Exponential Moving Average (DEMA)', 10, 'mid-price series'),
    (123, 'Double Exponential Moving Average (DEMA)', 14, 'mid-price series'),
    (124, 'Double Exponential Moving Average (DEMA)', 20, 'mid-price series'),
    (125, 'Triple Exponential Moving Average (TEMA)', 5, 'mid-price series'),
    (126, 'Triple Exponential Moving Average (TEMA)', 10, 'mid-price series'),
    (127, 'Triple Exponential Moving Average (TEMA)', 14, 'mid-price series'),
    (128, 'Triple Exponential Moving Average (TEMA)', 20, 'mid-price series'),
    (129, 'Triangular Moving Average (TRIMA)', 5, 'mid-price series'),
    (130, 'Triangular Moving Average (TRIMA)', 10, 'mid-price series'),
    (131, 'Triangular Moving Average (TRIMA)', 14, 'mid-price series'),
    (132, 'Triangular Moving Average (TRIMA)', 20, 'mid-price series'),
    (133, 'Hull Moving Average (HMA)', 5, 'mid-price series'),
    (134, 'Hull Moving Average (HMA)', 10, 'mid-price series'),
    (135, 'Hull Moving Average (HMA)', 14, 'mid-price series'),
    (136, 'Hull Moving Average (HMA)', 20, 'mid-price series'),
    (137, 'Kaufman Adaptive Moving Average (KAMA)', 5, 'mid-price series'),
    (138, 'Kaufman Adaptive Moving Average (KAMA)', 10, 'mid-price series'),
    (139, 'Kaufman Adaptive Moving Average (KAMA)', 14, 'mid-price series'),
    (140, 'Kaufman Adaptive Moving Average (KAMA)', 20, 'mid-price series'),
    (141, 'McGinley Dynamic', 5, 'mid-price series'),
    (142, 'McGinley Dynamic', 10, 'mid-price series'),
    (143, 'McGinley Dynamic', 14, 'mid-price series'),
    (144, 'McGinley Dynamic', 20, 'mid-price series'),
    (145, 'Arnaud Legoux Moving Average (ALMA)', 5, 'mid-price series'),
    (146, 'Arnaud Legoux Moving Average (ALMA)', 10, 'mid-price series'),
    (147, 'Arnaud Legoux Moving Average (ALMA)', 14, 'mid-price series'),
    (148, 'Arnaud Legoux Moving Average (ALMA)', 20, 'mid-price series'),
    (149, 'Zero-Lag Exponential Moving Average (ZLEMA)', 5, 'mid-price series'),
    (150, 'Zero-Lag Exponential Moving Average (ZLEMA)', 10, 'mid-price series'),
    (151, 'Zero-Lag Exponential Moving Average (ZLEMA)', 14, 'mid-price series'),
    (152, 'Zero-Lag Exponential Moving Average (ZLEMA)', 20, 'mid-price series'),
    (153, 'Mesa Adaptive Moving Average (MAMA)', 5, 'mid-price series'),
    (154, 'Mesa Adaptive Moving Average (MAMA)', 10, 'mid-price series'),
    (155, 'Mesa Adaptive Moving Average (MAMA)', 14, 'mid-price series'),
    (156, 'Mesa Adaptive Moving Average (MAMA)', 20, 'mid-price series'),
    (157, 'Tillson T3 Moving Average', 5, 'mid-price series'),
    (158, 'Tillson T3 Moving Average', 10, 'mid-price series'),
    (159, 'Tillson T3 Moving Average', 14, 'mid-price series'),
    (160, 'Tillson T3 Moving Average', 20, 'mid-price series'),
    (161, 'Least Squares Moving Average (LSMA)', 5, 'mid-price series'),
    (162, 'Least Squares Moving Average (LSMA)', 10, 'mid-price series'),
    (163, 'Least Squares Moving Average (LSMA)', 14, 'mid-price series'),
    (164, 'Least Squares Moving Average (LSMA)', 20, 'mid-price series'),
    (165, 'Relative Strength Index (RSI)', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (166, 'Relative Strength Index (RSI)', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (167, 'Relative Strength Index (RSI)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (168, 'Relative Strength Index (RSI)', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (169, 'Stochastic Oscillator', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (170, 'Stochastic Oscillator', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (171, 'Stochastic Oscillator', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (172, 'Stochastic Oscillator', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (173, 'Stochastic RSI', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (174, 'Stochastic RSI', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (175, 'Stochastic RSI', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (176, 'Stochastic RSI', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (177, 'Moving Average Convergence Divergence (MACD)', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (178, 'Moving Average Convergence Divergence (MACD)', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (179, 'Moving Average Convergence Divergence (MACD)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (180, 'Moving Average Convergence Divergence (MACD)', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (181, 'Percentage Price Oscillator (PPO)', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (182, 'Percentage Price Oscillator (PPO)', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (183, 'Percentage Price Oscillator (PPO)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (184, 'Percentage Price Oscillator (PPO)', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (185, 'Absolute Price Oscillator (APO)', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (186, 'Absolute Price Oscillator (APO)', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (187, 'Absolute Price Oscillator (APO)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (188, 'Absolute Price Oscillator (APO)', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (189, 'Rate of Change (ROC)', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (190, 'Rate of Change (ROC)', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (191, 'Rate of Change (ROC)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (192, 'Rate of Change (ROC)', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (193, 'Momentum (MOM)', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (194, 'Momentum (MOM)', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (195, 'Momentum (MOM)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (196, 'Momentum (MOM)', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (197, 'Chande Momentum Oscillator (CMO)', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (198, 'Chande Momentum Oscillator (CMO)', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (199, 'Chande Momentum Oscillator (CMO)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (200, 'Chande Momentum Oscillator (CMO)', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (201, 'True Strength Index (TSI)', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (202, 'True Strength Index (TSI)', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (203, 'True Strength Index (TSI)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (204, 'True Strength Index (TSI)', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (205, 'Williams %R', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (206, 'Williams %R', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (207, 'Williams %R', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (208, 'Williams %R', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (209, 'Commodity Channel Index (CCI)', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (210, 'Commodity Channel Index (CCI)', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (211, 'Commodity Channel Index (CCI)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (212, 'Commodity Channel Index (CCI)', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (213, 'Connors RSI', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (214, 'Connors RSI', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (215, 'Connors RSI', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (216, 'Connors RSI', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (217, 'Know Sure Thing (KST)', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (218, 'Know Sure Thing (KST)', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (219, 'Know Sure Thing (KST)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (220, 'Know Sure Thing (KST)', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (221, 'Elder Ray Index', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (222, 'Elder Ray Index', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (223, 'Elder Ray Index', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (224, 'Elder Ray Index', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (225, 'Awesome Oscillator', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (226, 'Awesome Oscillator', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (227, 'Awesome Oscillator', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (228, 'Awesome Oscillator', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (229, 'Average Directional Index (ADX)', 5, 'resampled mid-price bars unless stated'),
    (230, 'Average Directional Index (ADX)', 10, 'resampled mid-price bars unless stated'),
    (231, 'Average Directional Index (ADX)', 14, 'resampled mid-price bars unless stated'),
    (232, 'Average Directional Index (ADX)', 20, 'resampled mid-price bars unless stated'),
    (233, 'Average Directional Index Rating (ADXR)', 5, 'resampled mid-price bars unless stated'),
    (234, 'Average Directional Index Rating (ADXR)', 10, 'resampled mid-price bars unless stated'),
    (235, 'Average Directional Index Rating (ADXR)', 14, 'resampled mid-price bars unless stated'),
    (236, 'Average Directional Index Rating (ADXR)', 20, 'resampled mid-price bars unless stated'),
    (237, 'Directional Movement Index (DX)', 5, 'resampled mid-price bars unless stated'),
    (238, 'Directional Movement Index (DX)', 10, 'resampled mid-price bars unless stated'),
    (239, 'Directional Movement Index (DX)', 14, 'resampled mid-price bars unless stated'),
    (240, 'Directional Movement Index (DX)', 20, 'resampled mid-price bars unless stated'),
    (241, 'Plus Directional Indicator (+DI)', 5, 'resampled mid-price bars unless stated'),
    (242, 'Plus Directional Indicator (+DI)', 10, 'resampled mid-price bars unless stated'),
    (243, 'Plus Directional Indicator (+DI)', 14, 'resampled mid-price bars unless stated'),
    (244, 'Plus Directional Indicator (+DI)', 20, 'resampled mid-price bars unless stated'),
    (245, 'Minus Directional Indicator (-DI)', 5, 'resampled mid-price bars unless stated'),
    (246, 'Minus Directional Indicator (-DI)', 10, 'resampled mid-price bars unless stated'),
    (247, 'Minus Directional Indicator (-DI)', 14, 'resampled mid-price bars unless stated'),
    (248, 'Minus Directional Indicator (-DI)', 20, 'resampled mid-price bars unless stated'),
    (249, 'Aroon Indicator', 5, 'resampled mid-price bars unless stated'),
    (250, 'Aroon Indicator', 10, 'resampled mid-price bars unless stated'),
    (251, 'Aroon Indicator', 14, 'resampled mid-price bars unless stated'),
    (252, 'Aroon Indicator', 20, 'resampled mid-price bars unless stated'),
    (253, 'Aroon Oscillator', 5, 'resampled mid-price bars unless stated'),
    (254, 'Aroon Oscillator', 10, 'resampled mid-price bars unless stated'),
    (255, 'Aroon Oscillator', 14, 'resampled mid-price bars unless stated'),
    (256, 'Aroon Oscillator', 20, 'resampled mid-price bars unless stated'),
    (257, 'Vortex Indicator', 5, 'resampled mid-price bars unless stated'),
    (258, 'Vortex Indicator', 10, 'resampled mid-price bars unless stated'),
    (259, 'Vortex Indicator', 14, 'resampled mid-price bars unless stated'),
    (260, 'Vortex Indicator', 20, 'resampled mid-price bars unless stated'),
    (261, 'Supertrend', 5, 'resampled mid-price bars unless stated'),
    (262, 'Supertrend', 10, 'resampled mid-price bars unless stated'),
    (263, 'Supertrend', 14, 'resampled mid-price bars unless stated'),
    (264, 'Supertrend', 20, 'resampled mid-price bars unless stated'),
    (265, 'Parabolic SAR', 5, 'resampled mid-price bars unless stated'),
    (266, 'Parabolic SAR', 10, 'resampled mid-price bars unless stated'),
    (267, 'Parabolic SAR', 14, 'resampled mid-price bars unless stated'),
    (268, 'Parabolic SAR', 20, 'resampled mid-price bars unless stated'),
    (269, 'Choppiness Index', 5, 'resampled mid-price bars unless stated'),
    (270, 'Choppiness Index', 10, 'resampled mid-price bars unless stated'),
    (271, 'Choppiness Index', 14, 'resampled mid-price bars unless stated'),
    (272, 'Choppiness Index', 20, 'resampled mid-price bars unless stated'),
    (273, 'Linear Regression Slope', 5, 'resampled mid-price bars unless stated'),
    (274, 'Linear Regression Slope', 10, 'resampled mid-price bars unless stated'),
    (275, 'Linear Regression Slope', 14, 'resampled mid-price bars unless stated'),
    (276, 'Linear Regression Slope', 20, 'resampled mid-price bars unless stated'),
    (277, 'Trend Strength Index', 5, 'resampled mid-price bars unless stated'),
    (278, 'Trend Strength Index', 10, 'resampled mid-price bars unless stated'),
    (279, 'Trend Strength Index', 14, 'resampled mid-price bars unless stated'),
    (280, 'Trend Strength Index', 20, 'resampled mid-price bars unless stated'),
    (281, 'Average True Range (ATR)', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (282, 'Average True Range (ATR)', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (283, 'Average True Range (ATR)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (284, 'Average True Range (ATR)', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (285, 'Normalized Average True Range (NATR)', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (286, 'Normalized Average True Range (NATR)', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (287, 'Normalized Average True Range (NATR)', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (288, 'Normalized Average True Range (NATR)', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (289, 'Bollinger Bands', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (290, 'Bollinger Bands', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (291, 'Bollinger Bands', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (292, 'Bollinger Bands', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (293, 'Bollinger %B', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (294, 'Bollinger %B', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (295, 'Bollinger %B', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (296, 'Bollinger %B', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (297, 'Bollinger Band Width', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (298, 'Bollinger Band Width', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (299, 'Bollinger Band Width', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (300, 'Bollinger Band Width', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (301, 'Keltner Channels', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (302, 'Keltner Channels', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (303, 'Keltner Channels', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (304, 'Keltner Channels', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (305, 'Donchian Channels', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (306, 'Donchian Channels', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (307, 'Donchian Channels', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (308, 'Donchian Channels', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (309, 'Average Range', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (310, 'Average Range', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (311, 'Average Range', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (312, 'Average Range', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (313, 'Historical Volatility', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (314, 'Historical Volatility', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (315, 'Historical Volatility', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (316, 'Historical Volatility', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (317, 'Parkinson Volatility', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (318, 'Parkinson Volatility', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (319, 'Parkinson Volatility', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (320, 'Parkinson Volatility', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (321, 'Garman-Klass Volatility', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (322, 'Garman-Klass Volatility', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (323, 'Garman-Klass Volatility', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (324, 'Garman-Klass Volatility', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (325, 'Chaikin Volatility', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (326, 'Chaikin Volatility', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (327, 'Chaikin Volatility', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (328, 'Chaikin Volatility', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (329, 'Ulcer Index', 5, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (330, 'Ulcer Index', 10, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (331, 'Ulcer Index', 14, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (332, 'Ulcer Index', 20, 'mid-price; OHLC-style inputs need resampled mid-price bars'),
    (333, 'On-Balance Volume (OBV)', 5, 'actual trade volume required; unavailable in these CSVs'),
    (334, 'On-Balance Volume (OBV)', 10, 'actual trade volume required; unavailable in these CSVs'),
    (335, 'On-Balance Volume (OBV)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (336, 'On-Balance Volume (OBV)', 20, 'actual trade volume required; unavailable in these CSVs'),
    (337, 'Volume-Weighted Average Price (VWAP)', 5, 'actual trade volume required; unavailable in these CSVs'),
    (338, 'Volume-Weighted Average Price (VWAP)', 10, 'actual trade volume required; unavailable in these CSVs'),
    (339, 'Volume-Weighted Average Price (VWAP)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (340, 'Volume-Weighted Average Price (VWAP)', 20, 'actual trade volume required; unavailable in these CSVs'),
    (341, 'Volume Weighted Moving Average (VWMA)', 5, 'actual trade volume required; unavailable in these CSVs'),
    (342, 'Volume Weighted Moving Average (VWMA)', 10, 'actual trade volume required; unavailable in these CSVs'),
    (343, 'Volume Weighted Moving Average (VWMA)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (344, 'Volume Weighted Moving Average (VWMA)', 20, 'actual trade volume required; unavailable in these CSVs'),
    (345, 'Money Flow Index (MFI)', 5, 'actual trade volume required; unavailable in these CSVs'),
    (346, 'Money Flow Index (MFI)', 10, 'actual trade volume required; unavailable in these CSVs'),
    (347, 'Money Flow Index (MFI)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (348, 'Money Flow Index (MFI)', 20, 'actual trade volume required; unavailable in these CSVs'),
    (349, 'Chaikin Money Flow (CMF)', 5, 'actual trade volume required; unavailable in these CSVs'),
    (350, 'Chaikin Money Flow (CMF)', 10, 'actual trade volume required; unavailable in these CSVs'),
    (351, 'Chaikin Money Flow (CMF)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (352, 'Chaikin Money Flow (CMF)', 20, 'actual trade volume required; unavailable in these CSVs'),
    (353, 'Chaikin Accumulation/Distribution Line', 5, 'actual trade volume required; unavailable in these CSVs'),
    (354, 'Chaikin Accumulation/Distribution Line', 10, 'actual trade volume required; unavailable in these CSVs'),
    (355, 'Chaikin Accumulation/Distribution Line', 14, 'actual trade volume required; unavailable in these CSVs'),
    (356, 'Chaikin Accumulation/Distribution Line', 20, 'actual trade volume required; unavailable in these CSVs'),
    (357, 'Chaikin A/D Oscillator', 5, 'actual trade volume required; unavailable in these CSVs'),
    (358, 'Chaikin A/D Oscillator', 10, 'actual trade volume required; unavailable in these CSVs'),
    (359, 'Chaikin A/D Oscillator', 14, 'actual trade volume required; unavailable in these CSVs'),
    (360, 'Chaikin A/D Oscillator', 20, 'actual trade volume required; unavailable in these CSVs'),
    (361, 'Volume Price Trend (VPT)', 5, 'actual trade volume required; unavailable in these CSVs'),
    (362, 'Volume Price Trend (VPT)', 10, 'actual trade volume required; unavailable in these CSVs'),
    (363, 'Volume Price Trend (VPT)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (364, 'Volume Price Trend (VPT)', 20, 'actual trade volume required; unavailable in these CSVs'),
    (365, 'Price Volume Trend (PVT)', 5, 'actual trade volume required; unavailable in these CSVs'),
    (366, 'Price Volume Trend (PVT)', 10, 'actual trade volume required; unavailable in these CSVs'),
    (367, 'Price Volume Trend (PVT)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (368, 'Price Volume Trend (PVT)', 20, 'actual trade volume required; unavailable in these CSVs'),
    (369, 'Ease of Movement (EMV)', 5, 'actual trade volume required; unavailable in these CSVs'),
    (370, 'Ease of Movement (EMV)', 10, 'actual trade volume required; unavailable in these CSVs'),
    (371, 'Ease of Movement (EMV)', 14, 'actual trade volume required; unavailable in these CSVs'),
    (372, 'Ease of Movement (EMV)', 20, 'actual trade volume required; unavailable in these CSVs'),
    (373, 'Force Index', 5, 'actual trade volume required; unavailable in these CSVs'),
    (374, 'Force Index', 10, 'actual trade volume required; unavailable in these CSVs'),
    (375, 'Force Index', 14, 'actual trade volume required; unavailable in these CSVs'),
    (376, 'Force Index', 20, 'actual trade volume required; unavailable in these CSVs'),
    (377, 'Klinger Volume Oscillator', 5, 'actual trade volume required; unavailable in these CSVs'),
    (378, 'Klinger Volume Oscillator', 10, 'actual trade volume required; unavailable in these CSVs'),
    (379, 'Klinger Volume Oscillator', 14, 'actual trade volume required; unavailable in these CSVs'),
    (380, 'Klinger Volume Oscillator', 20, 'actual trade volume required; unavailable in these CSVs'),
    (381, 'Best Bid-Ask Spread', 5, 'directly computable from LOB snapshots'),
    (382, 'Best Bid-Ask Spread', 10, 'directly computable from LOB snapshots'),
    (383, 'Best Bid-Ask Spread', 14, 'directly computable from LOB snapshots'),
    (384, 'Best Bid-Ask Spread', 20, 'directly computable from LOB snapshots'),
    (385, 'Relative Bid-Ask Spread', 5, 'directly computable from LOB snapshots'),
    (386, 'Relative Bid-Ask Spread', 10, 'directly computable from LOB snapshots'),
    (387, 'Relative Bid-Ask Spread', 14, 'directly computable from LOB snapshots'),
    (388, 'Relative Bid-Ask Spread', 20, 'directly computable from LOB snapshots'),
    (389, 'Mid-Price', 5, 'directly computable from LOB snapshots'),
    (390, 'Mid-Price', 10, 'directly computable from LOB snapshots'),
    (391, 'Mid-Price', 14, 'directly computable from LOB snapshots'),
    (392, 'Mid-Price', 20, 'directly computable from LOB snapshots'),
    (393, 'Microprice', 5, 'directly computable from LOB snapshots'),
    (394, 'Microprice', 10, 'directly computable from LOB snapshots'),
    (395, 'Microprice', 14, 'directly computable from LOB snapshots'),
    (396, 'Microprice', 20, 'directly computable from LOB snapshots'),
    (397, 'Top-Level Queue Imbalance', 5, 'directly computable from LOB snapshots'),
    (398, 'Top-Level Queue Imbalance', 10, 'directly computable from LOB snapshots'),
    (399, 'Top-Level Queue Imbalance', 14, 'directly computable from LOB snapshots'),
    (400, 'Top-Level Queue Imbalance', 20, 'directly computable from LOB snapshots'),
    (401, 'Level-Wise Queue Imbalance', 5, 'directly computable from LOB snapshots'),
    (402, 'Level-Wise Queue Imbalance', 10, 'directly computable from LOB snapshots'),
    (403, 'Level-Wise Queue Imbalance', 14, 'directly computable from LOB snapshots'),
    (404, 'Level-Wise Queue Imbalance', 20, 'directly computable from LOB snapshots'),
    (405, 'Cumulative Depth Imbalance', 5, 'directly computable from LOB snapshots'),
    (406, 'Cumulative Depth Imbalance', 10, 'directly computable from LOB snapshots'),
    (407, 'Cumulative Depth Imbalance', 14, 'directly computable from LOB snapshots'),
    (408, 'Cumulative Depth Imbalance', 20, 'directly computable from LOB snapshots'),
    (409, 'Multi-Level Order Book Imbalance', 5, 'directly computable from LOB snapshots'),
    (410, 'Multi-Level Order Book Imbalance', 10, 'directly computable from LOB snapshots'),
    (411, 'Multi-Level Order Book Imbalance', 14, 'directly computable from LOB snapshots'),
    (412, 'Multi-Level Order Book Imbalance', 20, 'directly computable from LOB snapshots'),
    (413, 'Bid-Ask Depth Ratio', 5, 'directly computable from LOB snapshots'),
    (414, 'Bid-Ask Depth Ratio', 10, 'directly computable from LOB snapshots'),
    (415, 'Bid-Ask Depth Ratio', 14, 'directly computable from LOB snapshots'),
    (416, 'Bid-Ask Depth Ratio', 20, 'directly computable from LOB snapshots'),
    (417, 'Weighted Mid-Price', 5, 'directly computable from LOB snapshots'),
    (418, 'Weighted Mid-Price', 10, 'directly computable from LOB snapshots'),
    (419, 'Weighted Mid-Price', 14, 'directly computable from LOB snapshots'),
    (420, 'Weighted Mid-Price', 20, 'directly computable from LOB snapshots'),
    (421, 'Volume-Weighted Bid Price', 5, 'directly computable from LOB snapshots'),
    (422, 'Volume-Weighted Bid Price', 10, 'directly computable from LOB snapshots'),
    (423, 'Volume-Weighted Bid Price', 14, 'directly computable from LOB snapshots'),
    (424, 'Volume-Weighted Bid Price', 20, 'directly computable from LOB snapshots'),
    (425, 'Volume-Weighted Ask Price', 5, 'directly computable from LOB snapshots'),
    (426, 'Volume-Weighted Ask Price', 10, 'directly computable from LOB snapshots'),
    (427, 'Volume-Weighted Ask Price', 14, 'directly computable from LOB snapshots'),
    (428, 'Volume-Weighted Ask Price', 20, 'directly computable from LOB snapshots'),
    (429, 'Bid Depth Slope', 5, 'directly computable from LOB snapshots'),
    (430, 'Bid Depth Slope', 10, 'directly computable from LOB snapshots'),
    (431, 'Bid Depth Slope', 14, 'directly computable from LOB snapshots'),
    (432, 'Bid Depth Slope', 20, 'directly computable from LOB snapshots'),
    (433, 'Ask Depth Slope', 5, 'directly computable from LOB snapshots'),
    (434, 'Ask Depth Slope', 10, 'directly computable from LOB snapshots'),
    (435, 'Ask Depth Slope', 14, 'directly computable from LOB snapshots'),
    (436, 'Ask Depth Slope', 20, 'directly computable from LOB snapshots'),
    (437, 'Order Book Depth Concentration', 5, 'directly computable from LOB snapshots'),
    (438, 'Order Book Depth Concentration', 10, 'directly computable from LOB snapshots'),
    (439, 'Order Book Depth Concentration', 14, 'directly computable from LOB snapshots'),
    (440, 'Order Book Depth Concentration', 20, 'directly computable from LOB snapshots'),
    (441, 'Simple Return', 5, 'mid-price series'),
    (442, 'Simple Return', 10, 'mid-price series'),
    (443, 'Simple Return', 14, 'mid-price series'),
    (444, 'Simple Return', 20, 'mid-price series'),
    (445, 'Log Return', 5, 'mid-price series'),
    (446, 'Log Return', 10, 'mid-price series'),
    (447, 'Log Return', 14, 'mid-price series'),
    (448, 'Log Return', 20, 'mid-price series'),
    (449, 'Cumulative Return', 5, 'mid-price series'),
    (450, 'Cumulative Return', 10, 'mid-price series'),
    (451, 'Cumulative Return', 14, 'mid-price series'),
    (452, 'Cumulative Return', 20, 'mid-price series'),
    (453, 'Rolling Mean Return', 5, 'mid-price series'),
    (454, 'Rolling Mean Return', 10, 'mid-price series'),
    (455, 'Rolling Mean Return', 14, 'mid-price series'),
    (456, 'Rolling Mean Return', 20, 'mid-price series'),
    (457, 'Rolling Return Standard Deviation', 5, 'mid-price series'),
    (458, 'Rolling Return Standard Deviation', 10, 'mid-price series'),
    (459, 'Rolling Return Standard Deviation', 14, 'mid-price series'),
    (460, 'Rolling Return Standard Deviation', 20, 'mid-price series'),
    (461, 'Rolling Return Variance', 5, 'mid-price series'),
    (462, 'Rolling Return Variance', 10, 'mid-price series'),
    (463, 'Rolling Return Variance', 14, 'mid-price series'),
    (464, 'Rolling Return Variance', 20, 'mid-price series'),
    (465, 'Rolling Skewness', 5, 'mid-price series'),
    (466, 'Rolling Skewness', 10, 'mid-price series'),
    (467, 'Rolling Skewness', 14, 'mid-price series'),
    (468, 'Rolling Skewness', 20, 'mid-price series'),
    (469, 'Rolling Kurtosis', 5, 'mid-price series'),
    (470, 'Rolling Kurtosis', 10, 'mid-price series'),
    (471, 'Rolling Kurtosis', 14, 'mid-price series'),
    (472, 'Rolling Kurtosis', 20, 'mid-price series'),
    (473, 'Rolling Median Absolute Deviation', 5, 'mid-price series'),
    (474, 'Rolling Median Absolute Deviation', 10, 'mid-price series'),
    (475, 'Rolling Median Absolute Deviation', 14, 'mid-price series'),
    (476, 'Rolling Median Absolute Deviation', 20, 'mid-price series'),
    (477, 'Rolling Quantile Range', 5, 'mid-price series'),
    (478, 'Rolling Quantile Range', 10, 'mid-price series'),
    (479, 'Rolling Quantile Range', 14, 'mid-price series'),
    (480, 'Rolling Quantile Range', 20, 'mid-price series'),
    (481, 'Rolling Autocorrelation', 5, 'mid-price series'),
    (482, 'Rolling Autocorrelation', 10, 'mid-price series'),
    (483, 'Rolling Autocorrelation', 14, 'mid-price series'),
    (484, 'Rolling Autocorrelation', 20, 'mid-price series'),
    (485, 'Rolling Partial Autocorrelation', 5, 'mid-price series'),
    (486, 'Rolling Partial Autocorrelation', 10, 'mid-price series'),
    (487, 'Rolling Partial Autocorrelation', 14, 'mid-price series'),
    (488, 'Rolling Partial Autocorrelation', 20, 'mid-price series'),
    (489, 'Rolling Hurst Exponent', 5, 'mid-price series'),
    (490, 'Rolling Hurst Exponent', 10, 'mid-price series'),
    (491, 'Rolling Hurst Exponent', 14, 'mid-price series'),
    (492, 'Rolling Hurst Exponent', 20, 'mid-price series'),
    (493, 'Rolling Variance Ratio', 5, 'mid-price series'),
    (494, 'Rolling Variance Ratio', 10, 'mid-price series'),
    (495, 'Rolling Variance Ratio', 14, 'mid-price series'),
    (496, 'Rolling Variance Ratio', 20, 'mid-price series'),
    (497, 'Rolling Z-Score', 5, 'mid-price series'),
    (498, 'Rolling Z-Score', 10, 'mid-price series'),
    (499, 'Rolling Z-Score', 14, 'mid-price series'),
    (500, 'Rolling Z-Score', 20, 'mid-price series'),
)
LEVEL_PATTERN = re.compile(r"^(bid|ask)_(px|qty)_(\d+)$", re.IGNORECASE)


def load_indicator_specs():
    return list(INDICATOR_SPECS)

def detect_book(fieldnames):
    levels = {"bid": {}, "ask": {}}
    for field in fieldnames:
        match = LEVEL_PATTERN.match(field)
        if match:
            side, kind, index = match.groups()
            levels[side.lower()].setdefault(int(index), {})[kind.lower()] = field
    indices = sorted(set(levels["bid"]) & set(levels["ask"]))
    if not indices:
        raise ValueError("No paired bid and ask depth columns found")
    for side in ("bid", "ask"):
        for index in indices:
            if set(levels[side][index]) != {"px", "qty"}:
                raise ValueError(f"Incomplete {side} level {index}")
    return indices, levels


def safe_div(a, b):
    return a / (b + EPSILON)


def average(values):
    return sum(values) / len(values) if values else math.nan


def ema(values, period):
    if not values:
        return math.nan
    alpha = 2.0 / (period + 1.0)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


def sma(values):
    return average(values)


def wma(values):
    if not values:
        return math.nan
    weights = range(1, len(values) + 1)
    return sum(value * weight for value, weight in zip(values, weights)) / sum(weights)


def linear_slope(values):
    n = len(values)
    if n < 2:
        return math.nan
    xbar = (n - 1) / 2
    ybar = average(values)
    denominator = sum((i - xbar) ** 2 for i in range(n))
    return safe_div(sum((i - xbar) * (value - ybar) for i, value in enumerate(values)), denominator)


def rsi(values, period):
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    changes = changes[-period:]
    if not changes:
        return math.nan
    gains = sum(max(change, 0.0) for change in changes) / len(changes)
    losses = sum(max(-change, 0.0) for change in changes) / len(changes)
    if losses == 0:
        return 100.0 if gains > 0 else 50.0
    return 100.0 - 100.0 / (1.0 + gains / losses)


def rolling_stats(values):
    avg = average(values)
    variance = average([(v - avg) ** 2 for v in values])
    std = math.sqrt(max(variance, 0.0))
    if std == 0 or not math.isfinite(std):
        skew = 0.0
        kurtosis = 0.0
    else:
        skew = average([((v - avg) / std) ** 3 for v in values])
        kurtosis = average([((v - avg) / std) ** 4 for v in values]) - 3.0
    return avg, variance, std, skew, kurtosis


def lob_values(row, indices, levels):
    bids = [float(row[levels["bid"][i]["px"]]) for i in indices]
    bid_qty = [float(row[levels["bid"][i]["qty"]]) for i in indices]
    asks = [float(row[levels["ask"][i]["px"]]) for i in indices]
    ask_qty = [float(row[levels["ask"][i]["qty"]]) for i in indices]
    total_bid = sum(bid_qty)
    total_ask = sum(ask_qty)
    best_bid = bids[0]
    best_ask = asks[0]
    mid = (best_bid + best_ask) / 2.0
    microprice = safe_div(best_ask * bid_qty[0] + best_bid * ask_qty[0], bid_qty[0] + ask_qty[0])
    level_imbalance = [(b - a) / (b + a + EPSILON) for b, a in zip(bid_qty, ask_qty)]
    cumulative_imbalance = []
    running_bid = 0.0
    running_ask = 0.0
    bid_concentration = 0.0
    ask_concentration = 0.0
    for b, a in zip(bid_qty, ask_qty):
        running_bid += b
        running_ask += a
        cumulative_imbalance.append((running_bid - running_ask) / (running_bid + running_ask + EPSILON))
        bid_concentration += (b / (total_bid + EPSILON)) ** 2
        ask_concentration += (a / (total_ask + EPSILON)) ** 2
    distances_bid = [mid - price for price in bids]
    distances_ask = [price - mid for price in asks]
    bid_depth_slope = safe_div(sum((d - average(distances_bid)) * (q - average(bid_qty)) for d, q in zip(distances_bid, bid_qty)), sum((d - average(distances_bid)) ** 2 for d in distances_bid))
    ask_depth_slope = safe_div(sum((d - average(distances_ask)) * (q - average(ask_qty)) for d, q in zip(distances_ask, ask_qty)), sum((d - average(distances_ask)) ** 2 for d in distances_ask))
    raw = {
        "Best Bid-Ask Spread": best_ask - best_bid,
        "Relative Bid-Ask Spread": safe_div(best_ask - best_bid, mid),
        "Mid-Price": mid,
        "Microprice": microprice,
        "Top-Level Queue Imbalance": level_imbalance[0],
        "Cumulative Depth Imbalance": cumulative_imbalance[-1],
        "Multi-Level Order Book Imbalance": safe_div(total_bid - total_ask, total_bid + total_ask),
        "Bid-Ask Depth Ratio": safe_div(total_bid, total_ask),
        "Weighted Mid-Price": microprice,
        "Volume-Weighted Bid Price": safe_div(sum(p * q for p, q in zip(bids, bid_qty)), total_bid),
        "Volume-Weighted Ask Price": safe_div(sum(p * q for p, q in zip(asks, ask_qty)), total_ask),
        "Bid Depth Slope": bid_depth_slope,
        "Ask Depth Slope": ask_depth_slope,
        "Order Book Depth Concentration": bid_concentration + ask_concentration,
    }
    return raw, level_imbalance, cumulative_imbalance, mid


def ma_value(name, values, period):
    w = values[-period:]
    if not w:
        return math.nan
    if name == "Simple Moving Average (SMA)":
        return sma(w)
    if name in ("Exponential Moving Average (EMA)", "Mesa Adaptive Moving Average (MAMA)"):
        return ema(w, period)
    if name in ("Weighted Moving Average (WMA)", "Least Squares Moving Average (LSMA)"):
        return wma(w) if name.startswith("Weighted") else w[-1] if len(w) == 1 else w[-1] + linear_slope(w) * (len(w) - 1 - (len(w) - 1) / 2)
    if name in ("Wilder Moving Average (RMA)", "Smoothed Moving Average (SMMA)"):
        alpha = 1.0 / period
        out = w[0]
        for value in w[1:]:
            out = alpha * value + (1.0 - alpha) * out
        return out
    if name == "Double Exponential Moving Average (DEMA)":
        first = [ema(w[:i + 1], period) for i in range(len(w))]
        return 2.0 * ema(w, period) - ema(first, period)
    if name == "Triple Exponential Moving Average (TEMA)":
        first = [ema(w[:i + 1], period) for i in range(len(w))]
        second = [ema(first[:i + 1], period) for i in range(len(first))]
        return 3.0 * ema(w, period) - 3.0 * ema(first, period) + ema(second, period)
    if name == "Triangular Moving Average (TRIMA)":
        return sma([sma(w[:i + 1]) for i in range(len(w))])
    if name == "Hull Moving Average (HMA)":
        half = max(1, period // 2)
        root = max(1, int(math.sqrt(period)))
        intermediate = [2 * wma(w[:i + 1][-half:]) - wma(w[:i + 1]) for i in range(len(w))]
        return wma(intermediate[-root:])
    if name == "Kaufman Adaptive Moving Average (KAMA)":
        change = abs(w[-1] - w[0])
        volatility = sum(abs(w[i] - w[i - 1]) for i in range(1, len(w)))
        efficiency = safe_div(change, volatility)
        fast = 2.0 / 3.0
        slow = 2.0 / 31.0
        alpha = (efficiency * (fast - slow) + slow) ** 2
        out = w[0]
        for value in w[1:]:
            out += alpha * (value - out)
        return out
    if name == "McGinley Dynamic":
        out = w[0]
        for value in w[1:]:
            out += (value - out) / (period * (value / (out + EPSILON)) ** 4)
        return out
    if name == "Arnaud Legoux Moving Average (ALMA)":
        sigma = max(period / 6.0, EPSILON)
        center = 0.85 * (len(w) - 1)
        weights = [math.exp(-((i - center) ** 2) / (2 * sigma * sigma)) for i in range(len(w))]
        return safe_div(sum(v * wt for v, wt in zip(w, weights)), sum(weights))
    if name == "Zero-Lag Exponential Moving Average (ZLEMA)":
        lag = max(0, (period - 1) // 2)
        adjusted = [w[i] + (w[i] - w[max(0, i - lag)]) for i in range(len(w))]
        return ema(adjusted, period)
    if name == "Tillson T3 Moving Average":
        return ema([ema([ema(w[:j + 1], period) for j in range(i + 1)], period) for i in range(len(w))], period)
    return ema(w, period)


def indicator_value(name, period, prices, lob, level_imbalance, cumulative_imbalance):
    w = prices[-max(period, 2):]
    if not w:
        return math.nan
    close = w[-1]
    changes = [w[i] - w[i - 1] for i in range(1, len(w))]
    returns = [safe_div(w[i] - w[i - 1], w[i - 1]) for i in range(1, len(w))]
    high = max(w)
    low = min(w)
    span = high - low
    avg, variance, std, skew, kurtosis = rolling_stats(w)
    if name in ("Simple Moving Average (SMA)", "Exponential Moving Average (EMA)", "Weighted Moving Average (WMA)", "Wilder Moving Average (RMA)", "Smoothed Moving Average (SMMA)", "Double Exponential Moving Average (DEMA)", "Triple Exponential Moving Average (TEMA)", "Triangular Moving Average (TRIMA)", "Hull Moving Average (HMA)", "Kaufman Adaptive Moving Average (KAMA)", "McGinley Dynamic", "Arnaud Legoux Moving Average (ALMA)", "Zero-Lag Exponential Moving Average (ZLEMA)", "Mesa Adaptive Moving Average (MAMA)", "Tillson T3 Moving Average", "Least Squares Moving Average (LSMA)"):
        return ma_value(name, prices, period)
    if name == "Linear Regression Slope":
        return linear_slope(w)
    if name == "Relative Strength Index (RSI)":
        return rsi(prices, period)
    if name == "Stochastic RSI":
        rsi_values = [rsi(prices[:i + 1], period) for i in range(max(0, len(prices) - period), len(prices))]
        return 100.0 * safe_div(rsi_values[-1] - min(rsi_values), max(rsi_values) - min(rsi_values)) if rsi_values else math.nan
    if name in ("Stochastic Oscillator", "Williams %R", "Commodity Channel Index (CCI)"):
        if name == "Stochastic Oscillator":
            return 100.0 * safe_div(close - low, span)
        if name == "Williams %R":
            return -100.0 * safe_div(high - close, span)
        typical = w
        typical_avg = average(typical)
        mean_dev = average([abs(v - typical_avg) for v in typical])
        return safe_div(close - typical_avg, 0.015 * mean_dev)
    if name in ("Moving Average Convergence Divergence (MACD)", "Percentage Price Oscillator (PPO)", "Absolute Price Oscillator (APO)"):
        fast = ema(prices, max(2, period // 2))
        slow = ema(prices, period)
        diff = fast - slow
        if name == "Percentage Price Oscillator (PPO)":
            return 100.0 * safe_div(diff, slow)
        return diff
    if name in ("Rate of Change (ROC)", "Momentum (MOM)"):
        old = prices[-period - 1] if len(prices) > period else prices[0]
        return 100.0 * safe_div(close - old, old) if name.startswith("Rate") else close - old
    if name == "Chande Momentum Oscillator (CMO)":
        gains = sum(max(c, 0.0) for c in changes[-period:])
        losses = sum(max(-c, 0.0) for c in changes[-period:])
        return 100.0 * safe_div(gains - losses, gains + losses)
    if name == "True Strength Index (TSI)":
        return 100.0 * safe_div(sum(changes), sum(abs(c) for c in changes))
    if name == "Connors RSI":
        streak_series = []
        streak = 0
        for change in changes:
            if change > 0:
                streak = max(0, streak) + 1
            elif change < 0:
                streak = min(0, streak) - 1
            else:
                streak = 0
            streak_series.append(float(streak))
        rank = 100.0 * sum(abs(change) <= abs(changes[-1]) for change in changes[-period:]) / len(changes[-period:]) if changes else math.nan
        return average([rsi(prices, min(3, period)), rsi(streak_series, min(2, period)), rank])
    if name == "Know Sure Thing (KST)":
        return sum(safe_div(close - prices[max(0, len(prices) - min(period * m, len(prices)))], prices[max(0, len(prices) - min(period * m, len(prices)))]) * m for m in (1, 2, 3, 4))
    if name == "Elder Ray Index":
        return close - avg
    if name == "Awesome Oscillator":
        return sma(w[-min(5, len(w)):]) - sma(w)
    if name in ("Average Directional Index (ADX)", "Average Directional Index Rating (ADXR)", "Directional Movement Index (DX)", "Plus Directional Indicator (+DI)", "Minus Directional Indicator (-DI)", "Aroon Indicator", "Aroon Oscillator", "Vortex Indicator", "Supertrend", "Parabolic SAR", "Choppiness Index", "Trend Strength Index"):
        up = [max(c, 0.0) for c in changes]
        down = [max(-c, 0.0) for c in changes]
        plus = sum(up[-period:])
        minus = sum(down[-period:])
        dx = 100.0 * safe_div(abs(plus - minus), plus + minus)
        if name in ("Average Directional Index (ADX)", "Average Directional Index Rating (ADXR)", "Directional Movement Index (DX)", "Trend Strength Index"):
            return dx
        if name == "Plus Directional Indicator (+DI)":
            return 100.0 * safe_div(plus, sum(abs(c) for c in changes[-period:]))
        if name == "Minus Directional Indicator (-DI)":
            return 100.0 * safe_div(minus, sum(abs(c) for c in changes[-period:]))
        if name == "Aroon Indicator":
            return float(w.index(max(w))) / max(1, len(w) - 1) * 100.0
        if name == "Aroon Oscillator":
            return 100.0 * (w.index(max(w)) - w.index(min(w))) / max(1, len(w) - 1)
        if name == "Vortex Indicator":
            return safe_div(sum(abs(w[i] - w[i - 1]) for i in range(1, len(w))), sum(abs(c) for c in changes))
        if name == "Supertrend":
            return avg - 2.0 * std if close < avg else avg + 2.0 * std
        if name == "Parabolic SAR":
            return min(w) if changes and changes[-1] >= 0 else max(w)
        return 100.0 * math.log10(safe_div(sum(abs(c) for c in changes), span)) / math.log10(max(2, period)) if span > 0 else 0.0
    if name in ("Average True Range (ATR)", "Normalized Average True Range (NATR)", "Average Range"):
        atr = average([abs(c) for c in changes])
        if name == "Normalized Average True Range (NATR)":
            return 100.0 * safe_div(atr, close)
        if name == "Average Range":
            return span
        return atr
    if name in ("Bollinger Bands", "Bollinger %B", "Bollinger Band Width"):
        if name == "Bollinger Bands":
            return avg + 2.0 * std
        if name == "Bollinger %B":
            return safe_div(close - (avg - 2.0 * std), 4.0 * std)
        return safe_div(4.0 * std, avg)
    if name in ("Keltner Channels", "Donchian Channels"):
        return avg + (2.0 * std if name == "Keltner Channels" else 0.0)
    if name in ("Historical Volatility", "Parkinson Volatility", "Garman-Klass Volatility", "Chaikin Volatility"):
        if name == "Historical Volatility":
            return math.sqrt(max(0.0, average([r * r for r in returns])))
        if name == "Parkinson Volatility":
            return math.sqrt(max(0.0, math.log((high + EPSILON) / (low + EPSILON)) ** 2 / (4.0 * math.log(2.0))))
        if name == "Garman-Klass Volatility":
            return math.sqrt(max(0.0, 0.5 * math.log((high + EPSILON) / (low + EPSILON)) ** 2))
        return std
    if name == "Ulcer Index":
        drawdowns = [100.0 * (value - max(w[:i + 1])) / (max(w[:i + 1]) + EPSILON) for i, value in enumerate(w)]
        return math.sqrt(average([d * d for d in drawdowns]))
    if name in ("Simple Return", "Log Return", "Cumulative Return", "Rolling Mean Return", "Rolling Return Standard Deviation", "Rolling Return Variance", "Rolling Skewness", "Rolling Kurtosis", "Rolling Median Absolute Deviation", "Rolling Quantile Range", "Rolling Autocorrelation", "Rolling Partial Autocorrelation", "Rolling Hurst Exponent", "Rolling Variance Ratio", "Rolling Z-Score"):
        if name == "Simple Return":
            return returns[-1] if returns else math.nan
        if name == "Log Return":
            return math.log(close / w[-2]) if len(w) > 1 and close > 0 and w[-2] > 0 else math.nan
        if name == "Cumulative Return":
            return safe_div(close, w[0]) - 1.0
        if name == "Rolling Mean Return":
            return average(returns)
        if name == "Rolling Return Standard Deviation":
            return math.sqrt(max(0.0, average([(v - average(returns)) ** 2 for v in returns]))) if returns else math.nan
        if name == "Rolling Return Variance":
            return average([(v - average(returns)) ** 2 for v in returns]) if returns else math.nan
        if name == "Rolling Skewness":
            return skew
        if name == "Rolling Kurtosis":
            return kurtosis
        if name == "Rolling Median Absolute Deviation":
            med = sorted(w)[len(w) // 2]
            deviations = sorted(abs(v - med) for v in w)
            return deviations[len(deviations) // 2]
        if name == "Rolling Quantile Range":
            ordered = sorted(w)
            return ordered[int(0.9 * (len(ordered) - 1))] - ordered[int(0.1 * (len(ordered) - 1))]
        if name in ("Rolling Autocorrelation", "Rolling Partial Autocorrelation"):
            if len(returns) < 2:
                return math.nan
            left, right = returns[:-1], returns[1:]
            lm, rm = average(left), average(right)
            return safe_div(sum((a - lm) * (b - rm) for a, b in zip(left, right)), math.sqrt(sum((a - lm) ** 2 for a in left) * sum((b - rm) ** 2 for b in right)))
        if name == "Rolling Hurst Exponent":
            return math.log(max(std, EPSILON)) / math.log(max(2, len(w)))
        if name == "Rolling Variance Ratio":
            return safe_div(average([(w[i] - w[max(0, i - 2)]) ** 2 for i in range(1, len(w))]), 2.0 * average([c * c for c in changes])) if changes else math.nan
        return safe_div(close - avg, std)
    direct = lob.get(name)
    if direct is not None:
        return direct
    return math.nan


def make_output_specs(entries, indices):
    columns = []
    specs = []
    for number, name, period, availability in entries:
        key = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower()
        suffix = "base" if number <= 100 else f"p{period}"
        if availability == "directly computable from LOB snapshots" and name == "Level-Wise Queue Imbalance":
            for index in indices:
                col = f"{key}_level_{index}_{suffix}"
                columns.append(col)
                specs.append((number, name, period, availability, col, index))
        elif availability == "directly computable from LOB snapshots" and name == "Cumulative Depth Imbalance":
            for index in indices:
                col = f"{key}_through_level_{index}_{suffix}"
                columns.append(col)
                specs.append((number, name, period, availability, col, index))
        else:
            col = f"{key}_{suffix}"
            columns.append(col)
            specs.append((number, name, period, availability, col, None))
    return columns, specs


def make_raw_columns(depth):
    columns = ["symbol", "event_ts_ms", "recv_ts_ns", "update_id"]
    for side in ("bid", "ask"):
        for index in range(1, depth + 1):
            columns.extend((f"{side}_px_{index:03d}", f"{side}_qty_{index:03d}"))
    return columns


def process_file(source, entries):
    run_dir = source.parent
    metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
    symbol = source.name.removesuffix("_lob.bin").upper()
    symbol_metadata = metadata["symbols"][symbol]
    tick_size = Decimal(symbol_metadata["tick_size"])
    depth = int(symbol_metadata["stored_lob_depth_per_side"])
    record = struct.Struct("<qqq" + f"{depth}i{depth}f{depth}i{depth}f")
    if record.size != int(metadata["lob_record"]["record_size_bytes"]):
        raise ValueError(f"Record-size metadata mismatch for {source}")
    if source.stat().st_size % record.size:
        raise ValueError(f"Truncated LOB record in {source}")
    indices = list(range(1, depth + 1))
    fieldnames = make_raw_columns(depth)
    indices, levels = detect_book(fieldnames)
    derived_columns, specs = make_output_specs(entries, indices)
    destination_dir = OUTPUT_DIR / run_dir.relative_to(DATA_DIR)
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / f"{symbol}_indicators.csv"
    with source.open("rb") as input_file, destination.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames + derived_columns, extrasaction="ignore")
        writer.writeheader()
        history = deque(maxlen=256)
        book_history = deque(maxlen=256)
        rows = 0
        while True:
            data = input_file.read(record.size)
            if not data:
                break
            values = record.unpack(data)
            event_ts, recv_ts, update_id = values[:3]
            bid_ticks = values[3:3 + depth]
            bid_qty = values[3 + depth:3 + 2 * depth]
            ask_ticks = values[3 + 2 * depth:3 + 3 * depth]
            ask_qty = values[3 + 3 * depth:3 + 4 * depth]
            row = {"symbol": symbol, "event_ts_ms": event_ts, "recv_ts_ns": recv_ts, "update_id": update_id}
            for side, ticks, quantities in (("bid", bid_ticks, bid_qty), ("ask", ask_ticks, ask_qty)):
                for index, tick, quantity in zip(indices, ticks, quantities):
                    row[f"{side}_px_{index:03d}"] = format(Decimal(tick) * tick_size, "f")
                    row[f"{side}_qty_{index:03d}"] = quantity
            book, level_imbalance, cumulative_imbalance, mid = lob_values(row, indices, levels)
            history.append(mid)
            book_history.append((book, level_imbalance, cumulative_imbalance))
            prices = list(history)
            book_history_rows = list(book_history)
            features = {}
            for number, name, period, availability, column, level in specs:
                if availability == "actual trade volume required; unavailable in these CSVs":
                    features[column] = ""
                elif name == "Level-Wise Queue Imbalance":
                    position = indices.index(level)
                    samples = book_history_rows[-period:] if number > 100 else [(book, level_imbalance, cumulative_imbalance)]
                    features[column] = average([sample[1][position] for sample in samples])
                elif name == "Cumulative Depth Imbalance":
                    position = indices.index(level)
                    samples = book_history_rows[-period:] if number > 100 else [(book, level_imbalance, cumulative_imbalance)]
                    features[column] = average([sample[2][position] for sample in samples])
                elif availability == "directly computable from LOB snapshots" and number > 100:
                    samples = book_history_rows[-period:]
                    features[column] = average([sample[0][name] for sample in samples])
                else:
                    value = indicator_value(name, period, prices, book, level_imbalance, cumulative_imbalance)
                    features[column] = value if isinstance(value, str) or math.isfinite(value) else ""
            row.update(features)
            writer.writerow(row)
            rows += 1
    return source.name, str(destination), rows, depth


def main():
    if not 1 <= THREADS <= 8:
        raise ValueError("THREADS must be between 1 and 8")
    if not DATA_DIR.is_dir():
        raise FileNotFoundError(f"LOB data directory not found: {DATA_DIR}")
    sources = sorted(DATA_DIR.rglob("*_lob.bin"))
    if not sources:
        raise FileNotFoundError(f"No *_lob.bin files found in {DATA_DIR}")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for source in sources:
        symbol = source.name.removesuffix("_lob.bin").upper()
        target = OUTPUT_DIR / source.parent.relative_to(DATA_DIR) / f"{symbol}_indicators.csv"
        if target.exists():
            raise FileExistsError(f"Output already exists: {target}")
    entries = load_indicator_specs()
    with ThreadPoolExecutor(max_workers=min(THREADS, len(sources))) as executor:
        futures = [executor.submit(process_file, source, entries) for source in sources]
        for future in as_completed(futures):
            name, destination, rows, depth = future.result()
            print(f"{name}: {rows} records, {depth} levels per side -> {destination}")


if __name__ == "__main__":
    main()
