from supabase import create_client
import os

class TokenValidator:
    def __init__(self):
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        self.supabase = create_client(supabase_url, supabase_key)

    def validate_token(self, token: str) -> bool:
        try:
            # בדיקה האם הטוקן קיים בטבלת הטוקנים ופעיל
            result = self.supabase.table('api_tokens') \
                .select('*') \
                .eq('token', token) \
                .eq('is_active', True) \
                .execute()
            
            if len(result.data) == 0:
                return False
                
            # עדכון זמן השימוש האחרון
            token_id = result.data[0]['id']
            self.supabase.table('api_tokens') \
                .update({"last_used_at": "now()"}) \
                .eq('id', token_id) \
                .execute()
                
            return True
            
        except Exception as e:
            print(f"Error validating token: {str(e)}")
            return False 