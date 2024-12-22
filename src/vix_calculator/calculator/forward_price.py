from typing import Tuple
import pandas as pd
import numpy as np

def prepare_strike_ranges(near_calls: pd.DataFrame, near_puts: pd.DataFrame,
                        next_calls: pd.DataFrame, next_puts: pd.DataFrame,
                        F1: float, F2: float) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, float, float]:
    """
    Prepare strike ranges for variance calculation.
    
    Args:
        near_calls: Near-term call options
        near_puts: Near-term put options
        next_calls: Next-term call options
        next_puts: Next-term put options
        F1: Near-term forward price
        F2: Next-term forward price
    
    Returns:
        Tuple containing:
        - near-term strikes DataFrame
        - next-term strikes DataFrame
        - near-term strike differences
        - next-term strike differences
        - K0_1 (near-term central strike)
        - K0_2 (next-term central strike)
    """
    # Find K0 strikes
    stk_1_num = np.abs(np.array(near_calls.strike) - F1).argmin()
    stk_2_num = np.abs(np.array(next_calls.strike) - F2).argmin()
    
    # Get K0 values and reference rows
    K0_1 = near_calls.strike.iloc[stk_1_num]
    K0_2 = next_calls.strike.iloc[stk_2_num]
    K0_11 = near_calls.iloc[stk_1_num]
    K0_22 = next_calls.iloc[stk_2_num]
    
    # Near-term options selection
    strike_arg2 = near_puts[near_puts.strike == K0_1].index[0]
    PUTS = near_puts[0:strike_arg2]
    
    strike_arg3 = near_calls[near_calls.strike == K0_1].index[0] - 1
    strike_arg4 = near_calls.tail(1).index[0] + 1
    CALLS = near_calls[strike_arg3:strike_arg4]
    
    # Next-term options selection
    if len(next_puts[next_puts.strike == K0_2]) > 0:
        strike_arg6 = next_puts[next_puts.strike == K0_2].index[0]
    else:
        strike_arg6 = (next_puts.strike - K0_2).abs().argmin()
    PUTSW = next_puts[0:strike_arg6]
    
    if len(next_calls[next_calls.strike == K0_2]) > 0:
        strike_arg7 = next_calls[next_calls.strike == K0_2].index[0]
    else:
        strike_arg7 = (next_calls.strike - K0_2).abs().argmin()
    
    strike_arg8 = next_calls.tail(1).index[0]
    CALLSW = next_calls[strike_arg7:strike_arg8]
    
    # Combine strikes
    near0 = PUTS[::-1][['strike', 'option_mid', 'open_interest', 'option_volume']]
    near0 = pd.concat([near0, K0_11[['strike', 'option_mid', 'open_interest', 'option_volume']]])
    near0 = pd.concat([near0, CALLS[['strike', 'option_mid', 'open_interest', 'option_volume']]])
    near0 = pd.concat([near0, K0_11[['strike', 'option_mid', 'open_interest', 'option_volume']]])
    
    near0 = near0.sort_values('strike')
    near0 = near0.reset_index(drop=True)
    near_diff = near0.strike.diff().abs()
    near_diff.iloc[0] = near_diff.iloc[1]
    
    next0 = PUTSW[::-1][['strike', 'option_mid', 'open_interest', 'option_volume']]
    next0 = pd.concat([next0, K0_22[['strike', 'option_mid', 'open_interest', 'option_volume']]])
    next0 = pd.concat([next0, CALLSW[['strike', 'option_mid', 'open_interest', 'option_volume']]])
    next0 = pd.concat([next0, K0_22[['strike', 'option_mid', 'open_interest', 'option_volume']]])
    
    next0 = next0.sort_values('strike')
    next0 = next0.reset_index(drop=True)
    next_diff = next0.strike.diff().abs()
    next_diff.iloc[0] = next_diff.iloc[1]
    
    return near0, next0, near_diff, next_diff, K0_1, K0_2

def calculate_sigma(near_strikes: pd.DataFrame, next_strikes: pd.DataFrame,
                   near_diff: pd.Series, next_diff: pd.Series,
                   F1: float, F2: float, K0_1: float, K0_2: float,
                   T1: float, T2: float, R1: float, R2: float) -> Tuple[float, float]:
    """
    Calculate sigma1 and sigma2 components of VIX.
    
    Args:
        near_strikes: Near-term strike data
        next_strikes: Next-term strike data
        near_diff: Near-term strike differences
        next_diff: Next-term strike differences
        F1, F2: Forward prices
        K0_1, K0_2: Central strikes
        T1, T2: Times to expiration
        R1, R2: Interest rates
    
    Returns:
        Tuple of (sigma1, sigma2)
    """
    # Calculate variance contributions
    tmp1 = near_diff * (
        np.exp(R1 * T1) * near_strikes.option_mid
    ) / (near_strikes.strike * near_strikes.strike)
    
    tmp2 = next_diff * (
        np.exp(R2 * T2) * next_strikes.option_mid
    ) / (next_strikes.strike * next_strikes.strike)
    
    # Sum up contributions
    tmp11 = (tmp1.sum() * 2.0) / T1
    tmp22 = (tmp2.sum() * 2.0) / T2
    
    # Forward price components
    tmp3 = ((F1/K0_1 - 1)**2) / T1
    tmp4 = ((F2/K0_2 - 1)**2) / T2
    
    # Final sigma calculations
    sigma1 = tmp11 - tmp3
    sigma2 = tmp22 - tmp4
    
    return sigma1, sigma2


