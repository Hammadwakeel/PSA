#!/usr/bin/env python3
"""Quick test to verify the fix works"""
import sys
sys.path.insert(0, '/home/hasnain/Downloads/PSA')

from app.core.prompts import get_sql_agent_prompt

# Test with sample data
try:
    prompt = get_sql_agent_prompt(
        db_type="postgresql",
        governance_instructions=["Test RLS instruction"],
        schema_summary=["Table: customers | Columns: id, name"],
        lean_template={"test": "template"},
        feedback=None
    )
    print("✅ SUCCESS: Prompt generated without errors")
    print(f"Prompt length: {len(prompt)} characters")
    
    # Check that the fix is in place
    if "{{user_id}}" in prompt:
        print("✅ SUCCESS: Escaped braces {{user_id}} found in prompt")
    else:
        print("⚠️  WARNING: Escaped braces not found")
        
    if "{user_id}" in prompt.replace("{{user_id}}", ""):
        print("❌ ERROR: Unescaped {user_id} found!")
        sys.exit(1)
    else:
        print("✅ SUCCESS: No unescaped {user_id} found")
        
except NameError as e:
    print(f"❌ ERROR: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n✅ All tests passed!")

