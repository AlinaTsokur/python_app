"""Pipeline Processor Module - Centralized batch processing logic."""

import re
from core.parsing_engine import parse_raw_input, calculate_metrics
from core.report_generator import generate_xray, generate_composite


class PipelineProcessor:
    """Processes raw text batches through parsing, enrichment, and analysis."""
    
    def __init__(self, db_manager, config_loader):
        """
        Initialize with dependencies.
        
        Args:
            db_manager: DatabaseManager instance for DB operations
            config_loader: Callable that returns config dict (e.g., load_configurations)
        """
        self.db = db_manager
        self.config_loader = config_loader
    
    def process_batch(self, raw_text):
        """
        Central function to process raw text input (Tab 1 & Tab 3).
        
        Performs:
        1. Splitting by Exchange
        2. Parsing (parse_raw_input)
        3. Timestamp filtering/forwarding
        4. DB Enrichment (fetch_and_merge)
        5. Metric Calculation
        6. X-RAY Generation
        7. Composite Analysis (Grouping & Validation)
        
        Returns:
            tuple: (batch, orphan_errors)
                - batch (list): List of processed candle dictionaries
                - orphan_errors (list): List of validation error strings
        """
        config = self.config_loader()
        if not config:
            return [], ["Configuration load failed"]
        
        # 1. Split & Clean
        raw_chunks = re.split(r'(?m)^(?=\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2})', raw_text)
        raw_chunks = [x.strip() for x in raw_chunks if x.strip()]
        
        merged_groups = {}
        orphan_errors = []
        
        # 2. Iterate & Parse
        for chunk in raw_chunks:
            base_data = parse_raw_input(chunk)
            
            # STRICT CHECK: If TS is missing -> Error
            if not base_data.get('ts'):
                err = f"• {base_data.get('exchange', 'Unknown')} {base_data.get('symbol_clean', 'Unknown')} -> CRITICAL: Missing Timestamp/Exchange"
                orphan_errors.append(err)
                continue
            
            # Grouping for DB Merge
            key = (base_data.get('exchange'), base_data.get('symbol_clean'), 
                   base_data.get('tf'), base_data.get('ts'))
            
            if key not in merged_groups:
                merged_groups[key] = base_data
            else:
                existing = merged_groups[key]
                for k, v in base_data.items():
                    if v and (k not in existing or not existing[k]):
                        existing[k] = v
        
        local_batch = list(merged_groups.values())
        
        # 3. DB Enrichment
        final_batch_list = self.db.fetch_and_merge(local_batch)
        
        # 4. Metric Calculation & X-RAY
        temp_all_candles = []
        for raw_data in final_batch_list:
            full_data = calculate_metrics(raw_data, config)
            
            has_main = raw_data.get('buy_volume', 0) != 0
            if has_main:
                full_data['x_ray'] = generate_xray(full_data)
            else:
                full_data['x_ray'] = None
            
            temp_all_candles.append(full_data)
        
        # 5. Composite Analysis (Strict Mode)
        final_save_list = []
        composite_errors = []
        
        def get_comp_key(r):
            ts = str(r.get('ts', '')).replace('T', ' ')[:16]
            sym = str(r.get('symbol_clean', '')).upper()
            tf = str(r.get('tf', '')).upper()
            return (ts, sym, tf)
        
        comp_groups = {}
        for row in temp_all_candles:
            grp_key = get_comp_key(row)
            if grp_key not in comp_groups:
                comp_groups[grp_key] = []
            comp_groups[grp_key].append(row)
        
        # Separate Valid vs Orphans
        valid_groups = []
        orphans_groups = []
        
        for key, group in comp_groups.items():
            has_binance = any(c['exchange'] == 'Binance' for c in group)
            if has_binance:
                valid_groups.append(group)
            else:
                orphans_groups.append(group)
        
        # If orphans exist -> BLOCKING ERROR
        if orphans_groups:
            for grp in orphans_groups:
                orphan = grp[0]
                
                best_match = None
                min_diff = 3
                
                o_ts = get_comp_key(orphan)[0]
                o_sym = get_comp_key(orphan)[1]
                o_tf = get_comp_key(orphan)[2]
                
                for v_grp in valid_groups:
                    target = next((c for c in v_grp if c['exchange'] == 'Binance'), v_grp[0])
                    t_ts = get_comp_key(target)[0]
                    t_sym = get_comp_key(target)[1]
                    t_tf = get_comp_key(target)[2]
                    
                    curr_diff = 0
                    if o_ts != t_ts:
                        curr_diff += 1
                    if o_sym != t_sym:
                        curr_diff += 1
                    if o_tf != t_tf:
                        curr_diff += 1
                    
                    if curr_diff < min_diff:
                        min_diff = curr_diff
                        best_match = target
                
                err_msg = f"• {orphan.get('exchange')} {o_sym} {o_ts}"
                members_str = ", ".join([f"{m.get('exchange')}" for m in grp])
                err_msg += f" [Группа: {members_str}]"
                
                if best_match:
                    reasons = []
                    bm_ts = get_comp_key(best_match)[0]
                    bm_sym = get_comp_key(best_match)[1]
                    bm_tf = get_comp_key(best_match)[2]
                    
                    if o_ts != bm_ts:
                        reasons.append(f"Время ({o_ts} vs {bm_ts})")
                    if o_sym != bm_sym:
                        reasons.append(f"Тикер ({o_sym} vs {bm_sym})")
                    if o_tf != bm_tf:
                        reasons.append(f"ТФ ({o_tf} vs {bm_tf})")
                    
                    err_msg += f" -> Не совпало с Binance: {', '.join(reasons)}"
                else:
                    err_msg += " -> Не найдено пары на Binance (проверьте все параметры)"
                
                composite_errors.append(err_msg)
            
            total_errors = orphan_errors + composite_errors
            return [], total_errors
        
        else:
            # No orphans - process valid groups
            for group in valid_groups:
                target_candle = next((c for c in group if c['exchange'] == 'Binance'), None)
                if not target_candle:
                    target_candle = group[0]
                
                if target_candle:
                    unique_exchanges = set(r['exchange'] for r in group)
                    if len(unique_exchanges) >= 3:
                        comp_report = generate_composite(group)
                        target_candle['x_ray_composite'] = comp_report
                    
                    final_save_list.append(target_candle)
        
        return final_save_list, orphan_errors
