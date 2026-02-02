"""Database Manager Module - CRUD operations for candles table."""

import re
import pandas as pd
from datetime import datetime, time


class DatabaseManager:
    """Manages database operations for candles table."""
    
    def __init__(self, supabase_client):
        """Initialize with Supabase client instance."""
        self.supabase = supabase_client
    
    def save_candles_batch(self, candles_data):
        """
        Save batch of candles to database with upsert.
        Handles missing columns automatically.
        """
        if not candles_data:
            return True
        
        # Deep copy to allow modification during retries
        current_data = [c.copy() for c in candles_data]
        
        # Ensure note exists and remove ID to rely on composite key upsert
        for row in current_data:
            if 'note' not in row:
                row['note'] = ""
            # Remove 'id' to prevent "null value in column id" error during mixed batch upserts
            row.pop('id', None)
        
        # Attempt loop
        attempt = 0
        max_attempts = 20  # Enough for many missing metrics
        dropped_columns = []
        
        while attempt < max_attempts:
            try:
                # Upsert WITHOUT ignore_duplicates to allow UPDATES
                res = self.supabase.table('candles').upsert(
                    current_data,
                    on_conflict='exchange,symbol_clean,tf,ts'
                ).execute()
                
                return True
            except Exception as e:
                err_str = str(e)
                # Detect column error (PGRST204)
                match = re.search(r"Could not find the '(\w+)' column", err_str)
                if match:
                    bad_col = match.group(1)
                    if bad_col not in dropped_columns:
                        dropped_columns.append(bad_col)
                        # Remove this column from all rows
                        for row in current_data:
                            row.pop(bad_col, None)
                    else:
                        # Loop detected
                        raise Exception(f"Column loop detected on {bad_col}: {e}")
                    attempt += 1
                else:
                    # Other error
                    raise e
        
        raise Exception("Failed to save after max attempts removing columns")
    
    def load_candles(self, limit=100, start_date=None, end_date=None, tfs=None, symbols=None):
        """
        Load candles from database with optional filters.
        Returns pandas DataFrame.
        """
        query = self.supabase.table('candles').select("*").order('ts', desc=True)
        
        if start_date:
            query = query.gte('ts', start_date.isoformat())
        if end_date:
            # End date inclusive (until end of day)
            end_dt = datetime.combine(end_date, time(23, 59, 59))
            query = query.lte('ts', end_dt.isoformat())
        
        if tfs and len(tfs) > 0:
            # Case-insensitive filter hack: add both cases
            tfs_extended = list(set(tfs + [t.upper() for t in tfs] + [t.lower() for t in tfs]))
            query = query.in_('tf', tfs_extended)
        
        if symbols and len(symbols) > 0:
            # Фильтр по символам (активам) - используем symbol_clean
            query = query.in_('symbol_clean', symbols)
        
        res = query.limit(limit).execute()
        return pd.DataFrame(res.data) if res.data else pd.DataFrame()
    
    def get_unique_symbols(self):
        """Получить список уникальных символов (активов) из БД."""
        res = self.supabase.table('candles').select('symbol_clean').execute()
        if res.data:
            symbols = list(set(row['symbol_clean'] for row in res.data if row.get('symbol_clean')))
            return sorted(symbols)
        return []
    
    def delete_candles(self, ids):
        """Delete candles by IDs."""
        self.supabase.table('candles').delete().in_('id', ids).execute()
        return True
    
    def update_candle(self, id, changes):
        """Update single candle by ID."""
        self.supabase.table('candles').update(changes).eq('id', id).execute()
        return True
    
    def fetch_and_merge(self, batch_data):
        """
        Fetch existing candles from DB and merge with new batch data.
        1. Find existing candles by (exchange, symbol, tf, ts)
        2. Merge new data into existing (fill empty fields)
        Returns merged list.
        """
        if not batch_data:
            return []
        
        def get_merge_key(ex, sym, tf, ts):
            """Normalize key for reliable matching."""
            clean_ts = str(ts).replace('T', ' ')[:16]
            return (ex, sym, tf, clean_ts)
        
        # 1. Group by (exchange, symbol, tf) for optimized queries
        groups = {}  # (ex, sym, tf) -> [ts_list]
        for row in batch_data:
            key = (row.get('exchange'), row.get('symbol_clean'), row.get('tf'))
            if key not in groups:
                groups[key] = []
            groups[key].append(row.get('ts'))
        
        db_map = {}  # (ex, sym, tf, ts) -> db_row
        
        # 2. Batch fetching from DB
        for (ex, sym, tf), ts_list in groups.items():
            if not ts_list:
                continue
            min_ts = min(ts_list)
            max_ts = max(ts_list)
            
            res = self.supabase.table('candles')\
                .select("*")\
                .eq('exchange', ex)\
                .eq('symbol_clean', sym)\
                .eq('tf', tf)\
                .gte('ts', min_ts)\
                .lte('ts', max_ts)\
                .execute()
            
            if res.data:
                for db_row in res.data:
                    k = get_merge_key(
                        db_row.get('exchange'),
                        db_row.get('symbol_clean'),
                        db_row.get('tf'),
                        db_row.get('ts')
                    )
                    db_map[k] = db_row
        
        # 3. Merge
        merged_batch = []
        for new_row in batch_data:
            k = get_merge_key(
                new_row.get('exchange'),
                new_row.get('symbol_clean'),
                new_row.get('tf'),
                new_row.get('ts')
            )
            existing = db_map.get(k)
            
            if existing:
                # Merge strategy: new data fills empty fields in existing
                combined = existing.copy()
                
                for key, val in new_row.items():
                    existing_val = combined.get(key)
                    is_existing_empty = (existing_val is None) or \
                        (isinstance(existing_val, (int, float)) and existing_val == 0)
                    
                    if is_existing_empty:
                        combined[key] = val
                
                merged_batch.append(combined)
            else:
                merged_batch.append(new_row)
        
        return merged_batch


# Backward compatibility - standalone functions that use global supabase
# These will be deprecated in favor of class methods
def save_candles_batch(supabase, candles_data):
    """Backward compat wrapper."""
    db = DatabaseManager(supabase)
    return db.save_candles_batch(candles_data)


def load_candles_db(supabase, limit=100, start_date=None, end_date=None, tfs=None):
    """Backward compat wrapper."""
    db = DatabaseManager(supabase)
    return db.load_candles(limit, start_date, end_date, tfs)


def delete_candles_db(supabase, ids):
    """Backward compat wrapper."""
    db = DatabaseManager(supabase)
    return db.delete_candles(ids)


def update_candle_db(supabase, id, changes):
    """Backward compat wrapper."""
    db = DatabaseManager(supabase)
    return db.update_candle(id, changes)


def fetch_and_merge_db(supabase, batch_data, config=None):
    """
    Backward compat wrapper.
    Note: config param is preserved for signature compat but not used.
    """
    db = DatabaseManager(supabase)
    return db.fetch_and_merge(batch_data)
