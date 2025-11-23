# backend/core/verieql_integration.py

import re
import subprocess
import json
import tempfile
import os
from typing import Dict, Any, Optional, Tuple

class VeriEQLVerifier:
    """Interface to VeriEQL bounded equivalence checker"""
    
    def __init__(self, verieql_path: str = None):
        self.verieql_path = verieql_path or self._find_verieql()
        self.temp_dir = tempfile.gettempdir()
        
    def verify(self, sql1: str, sql2: str, bound: int = 20, timeout: int = 600) -> Dict[str, Any]:
        """
        Verify SQL equivalence using VeriEQL
        
        Returns:
            {
                'result': 'equivalent' | 'not_equivalent' | 'unknown',
                'bound': int,
                'time_ms': float,
                'counterexample': Optional[str]
            }
        """
        
        # Create temporary files for SQL queries
        sql1_file = os.path.join(self.temp_dir, f'query1_{id(sql1)}.sql')
        sql2_file = os.path.join(self.temp_dir, f'query2_{id(sql2)}.sql')
        
        try:
            # Write queries to files
            with open(sql1_file, 'w') as f:
                f.write(sql1)
            with open(sql2_file, 'w') as f:
                f.write(sql2)
            
            # Call VeriEQL
            result = self._run_verieql(sql1_file, sql2_file, bound, timeout)
            
            return result
            
        finally:
            # Cleanup
            if os.path.exists(sql1_file):
                os.remove(sql1_file)
            if os.path.exists(sql2_file):
                os.remove(sql2_file)
    
    def _run_verieql(self, sql1_file: str, sql2_file: str, bound: int, timeout: int) -> Dict[str, Any]:
        """Execute VeriEQL verification"""
        
        import time
        start_time = time.time()
        
        try:
            cmd = [
                self.verieql_path,
                '--query1', sql1_file,
                '--query2', sql2_file,
                '--bound', str(bound),
                '--timeout', str(timeout)
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout + 10
            )
            
            elapsed = (time.time() - start_time) * 1000
            
            # Parse VeriEQL output
            return self._parse_verieql_output(result.stdout, result.stderr, elapsed)
            
        except subprocess.TimeoutExpired:
            return {
                'result': 'timeout',
                'bound': bound,
                'time_ms': (time.time() - start_time) * 1000,
                'counterexample': None
            }
        except Exception as e:
            return {
                'result': 'error',
                'error': str(e),
                'time_ms': (time.time() - start_time) * 1000
            }
    
    def _parse_verieql_output(self, stdout: str, stderr: str, elapsed: float) -> Dict[str, Any]:
        """Parse VeriEQL output"""
        
        result = {
            'result': 'unknown',
            'bound': 20,
            'time_ms': elapsed,
            'counterexample': None
        }
        
        # Parse standard output for result
        if 'EQUIVALENT' in stdout.upper():
            result['result'] = 'equivalent'
        elif 'NOT EQUIVALENT' in stdout.upper() or 'COUNTEREXAMPLE' in stdout.upper():
            result['result'] = 'not_equivalent'
            # Extract counterexample if available
            if 'COUNTEREXAMPLE' in stdout:
                result['counterexample'] = self._extract_counterexample(stdout)
        
        # Extract bound information
        bound_match = re.search(r'bound[\\s=]+(\d+)', stdout, re.IGNORECASE)
        if bound_match:
            result['bound'] = int(bound_match.group(1))
        
        return result
    
    def _extract_counterexample(self, output: str) -> str:
        """Extract counterexample from VeriEQL output"""
        
        # Look for counterexample section
        match = re.search(r'COUNTEREXAMPLE:(.+?)(?=\\n(?:SUMMARY|$))', output, re.DOTALL)
        if match:
            return match.group(1).strip()
        
        return None
    
    def _find_verieql(self) -> str:
        """Find VeriEQL executable in system"""
        
        import shutil
        
        verieql = shutil.which('verieql')
        if verieql:
            return verieql
        
        # Try common installation paths
        common_paths = [
            '/usr/local/bin/verieql',
            '/opt/verieql/bin/verieql',
            os.path.expanduser('~/verieql/bin/verieql')
        ]
        
        for path in common_paths:
            if os.path.exists(path):
                return path
        
        raise FileNotFoundError("VeriEQL not found. Please install it or specify path.")
