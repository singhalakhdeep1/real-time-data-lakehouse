"""
Security Policies for Real-time Data Lakehouse
Manages access control, encryption, and data governance
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum


class Permission(Enum):
    """Data access permissions"""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"


class DataClassification(Enum):
    """Data sensitivity classification"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


@dataclass
class SecurityPolicy:
    """Security policy definition"""
    id: str
    name: str
    description: str
    resource_type: str  # dataset, pipeline, catalog
    resource_pattern: str
    classification: DataClassification
    permissions: List[Permission]
    enabled: bool
    created_at: str


@dataclass
class AccessRule:
    """Access control rule"""
    id: str
    principal: str  # user or role
    resource: str
    permissions: List[Permission]
    conditions: Dict[str, Any]
    expires_at: Optional[str]


class SecurityManager:
    """Manages security policies and access control"""
    
    def __init__(self):
        self.policies: Dict[str, SecurityPolicy] = {}
        self.access_rules: Dict[str, AccessRule] = {}
        self.encryption_keys: Dict[str, str] = {}
        self.audit_log: List[Dict] = []
        self.security_file = Path("security/security_policies.json")
        self.security_file.parent.mkdir(exist_ok=True)
        
    def create_policy(self, name: str, description: str, resource_type: str, 
                     resource_pattern: str, classification: DataClassification, 
                     permissions: List[Permission]) -> str:
        """Create a new security policy"""
        policy_id = self._generate_id(name)
        
        policy = SecurityPolicy(
            id=policy_id,
            name=name,
            description=description,
            resource_type=resource_type,
            resource_pattern=resource_pattern,
            classification=classification,
            permissions=permissions,
            enabled=True,
            created_at=datetime.now().isoformat()
        )
        
        self.policies[policy_id] = policy
        self._log_audit("policy_created", {"policy_id": policy_id, "name": name})
        
        return policy_id
    
    def grant_access(self, principal: str, resource: str, permissions: List[Permission], 
                    conditions: Dict[str, Any] = None, expires_at: str = None) -> str:
        """Grant access to a resource"""
        rule_id = self._generate_id(f"access_{principal}_{resource}")
        
        rule = AccessRule(
            id=rule_id,
            principal=principal,
            resource=resource,
            permissions=permissions,
            conditions=conditions or {},
            expires_at=expires_at
        )
        
        self.access_rules[rule_id] = rule
        self._log_audit("access_granted", {"rule_id": rule_id, "principal": principal, "resource": resource})
        
        return rule_id
    
    def revoke_access(self, principal: str, resource: str) -> bool:
        """Revoke access to a resource"""
        for rule_id, rule in list(self.access_rules.items()):
            if rule.principal == principal and rule.resource == resource:
                del self.access_rules[rule_id]
                self._log_audit("access_revoked", {"principal": principal, "resource": resource})
                return True
        return False
    
    def check_access(self, principal: str, resource: str, permission: Permission) -> bool:
        """Check if principal has permission to access resource"""
        # Check if any rule grants access
        for rule in self.access_rules.values():
            if rule.principal == principal and rule.resource == resource:
                # Check if rule is expired
                if rule.expires_at:
                    expiry = datetime.fromisoformat(rule.expires_at)
                    if datetime.now() > expiry:
                        continue
                
                # Check if permission is granted
                if permission in rule.permissions or Permission.ADMIN in rule.permissions:
                    return True
                
                # Check conditions
                if self._evaluate_conditions(rule.conditions):
                    return permission in rule.permissions
        
        return False
    
    def _evaluate_conditions(self, conditions: Dict[str, Any]) -> bool:
        """Evaluate access rule conditions"""
        # Simplified condition evaluation
        # In production, this would support complex logic
        if not conditions:
            return True
        
        # Example: time-based access
        if "time_range" in conditions:
            current_hour = datetime.now().hour
            start, end = conditions["time_range"]
            if not (start <= current_hour <= end):
                return False
        
        # Example: IP-based access
        if "allowed_ips" in conditions:
            # Would check actual IP in production
            pass
        
        return True
    
    def encrypt_data(self, data: str, key_id: str = "default") -> str:
        """Encrypt data (simplified - use proper encryption in production)"""
        # In production, use AES-256 or similar
        key = self.encryption_keys.get(key_id, self._generate_key())
        self.encryption_keys[key_id] = key
        
        # Simple XOR encryption for demonstration
        encrypted = ''.join(chr(ord(c) ^ ord(key[i % len(key)])) for i, c in enumerate(data))
        return encrypted
    
    def decrypt_data(self, encrypted: str, key_id: str = "default") -> str:
        """Decrypt data"""
        key = self.encryption_keys.get(key_id)
        if not key:
            raise ValueError(f"Encryption key {key_id} not found")
        
        decrypted = ''.join(chr(ord(c) ^ ord(key[i % len(key)])) for i, c in enumerate(encrypted))
        return decrypted
    
    def classify_data(self, data: Dict[str, Any]) -> DataClassification:
        """Auto-classify data based on content"""
        # Check for PII
        pii_fields = ['ssn', 'social_security', 'credit_card', 'email', 'phone']
        if any(field in data for field in pii_fields):
            return DataClassification.RESTRICTED
        
        # Check for financial data
        financial_fields = ['account_number', 'balance', 'transaction', 'salary']
        if any(field in data for field in financial_fields):
            return DataClassification.CONFIDENTIAL
        
        # Check for internal data
        internal_fields = ['employee_id', 'internal', 'confidential']
        if any(field in data for field in internal_fields):
            return DataClassification.INTERNAL
        
        return DataClassification.PUBLIC
    
    def apply_retention_policy(self, dataset_id: str, retention_days: int) -> bool:
        """Apply data retention policy"""
        # In production, this would delete old data
        self._log_audit("retention_applied", {
            "dataset_id": dataset_id,
            "retention_days": retention_days
        })
        return True
    
    def get_audit_log(self, limit: int = 100) -> List[Dict]:
        """Get audit log entries"""
        return self.audit_log[-limit:]
    
    def _log_audit(self, action: str, details: Dict[str, Any]):
        """Log security event to audit trail"""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "details": details
        }
        self.audit_log.append(entry)
    
    def _generate_id(self, name: str) -> str:
        """Generate unique ID"""
        import hashlib
        return hashlib.md5(f"{name}{datetime.now().isoformat()}".encode()).hexdigest()[:16]
    
    def _generate_key(self) -> str:
        """Generate encryption key (simplified)"""
        import secrets
        return secrets.token_hex(32)
    
    def save_policies(self):
        """Save security policies to file"""
        policies_data = {
            "policies": [asdict(p) for p in self.policies.values()],
            "access_rules": [asdict(r) for r in self.access_rules.values()],
            "audit_log": self.audit_log[-1000],  # Keep last 1000 entries
            "saved_at": datetime.now().isoformat()
        }
        self.security_file.write_text(json.dumps(policies_data, indent=2))
    
    def load_policies(self):
        """Load security policies from file"""
        if self.security_file.exists():
            policies_data = json.loads(self.security_file.read_text())
            
            for policy_data in policies_data["policies"]:
                policy_data["classification"] = DataClassification(policy_data["classification"])
                policy_data["permissions"] = [Permission(p) for p in policy_data["permissions"]]
                policy = SecurityPolicy(**policy_data)
                self.policies[policy.id] = policy
            
            for rule_data in policies_data["access_rules"]:
                rule_data["permissions"] = [Permission(p) for p in rule_data["permissions"]]
                rule = AccessRule(**rule_data)
                self.access_rules[rule.id] = rule
            
            self.audit_log = policies_data.get("audit_log", [])


class AutoScaler:
    """Auto-scaling configuration for data lakehouse resources"""
    
    def __init__(self):
        self.scaling_policies: Dict[str, Dict] = {}
        self.current_capacity: Dict[str, int] = {}
        
    def create_scaling_policy(self, resource_id: str, min_capacity: int, max_capacity: int, 
                             target_cpu: float, target_memory: float) -> str:
        """Create auto-scaling policy for a resource"""
        policy = {
            "resource_id": resource_id,
            "min_capacity": min_capacity,
            "max_capacity": max_capacity,
            "target_cpu": target_cpu,
            "target_memory": target_memory,
            "scale_up_cooldown": 300,  # 5 minutes
            "scale_down_cooldown": 600,  # 10 minutes
            "created_at": datetime.now().isoformat()
        }
        
        self.scaling_policies[resource_id] = policy
        self.current_capacity[resource_id] = min_capacity
        
        return resource_id
    
    def evaluate_scaling(self, resource_id: str, current_cpu: float, current_memory: float) -> Dict[str, Any]:
        """Evaluate if scaling is needed"""
        if resource_id not in self.scaling_policies:
            return {"action": "none", "reason": "No scaling policy"}
        
        policy = self.scaling_policies[resource_id]
        current = self.current_capacity[resource_id]
        
        # Scale up if CPU or memory exceeds target
        if current_cpu > policy["target_cpu"] or current_memory > policy["target_memory"]:
            if current < policy["max_capacity"]:
                new_capacity = min(current + 1, policy["max_capacity"])
                self.current_capacity[resource_id] = new_capacity
                return {
                    "action": "scale_up",
                    "from": current,
                    "to": new_capacity,
                    "reason": f"CPU ({current_cpu}%) or memory ({current_memory}%) exceeds target"
                }
        
        # Scale down if CPU and memory are well below target
        if (current_cpu < policy["target_cpu"] * 0.5 and 
            current_memory < policy["target_memory"] * 0.5):
            if current > policy["min_capacity"]:
                new_capacity = max(current - 1, policy["min_capacity"])
                self.current_capacity[resource_id] = new_capacity
                return {
                    "action": "scale_down",
                    "from": current,
                    "to": new_capacity,
                    "reason": f"CPU ({current_cpu}%) and memory ({current_memory}%) below threshold"
                }
        
        return {"action": "none", "reason": "Within target thresholds"}
    
    def get_capacity_status(self) -> Dict[str, Any]:
        """Get current capacity status for all resources"""
        return {
            "resources": [
                {
                    "resource_id": resource_id,
                    "current_capacity": self.current_capacity[resource_id],
                    "policy": self.scaling_policies[resource_id]
                }
                for resource_id in self.scaling_policies.keys()
            ],
            "timestamp": datetime.now().isoformat()
        }


if __name__ == "__main__":
    # Example usage
    security = SecurityManager()
    
    # Create security policies
    security.create_policy(
        name="PII Protection",
        description="Protect personally identifiable information",
        resource_type="dataset",
        resource_pattern="*users*",
        classification=DataClassification.RESTRICTED,
        permissions=[Permission.READ, Permission.WRITE]
    )
    
    # Grant access
    security.grant_access(
        principal="data-analyst",
        resource="user_events",
        permissions=[Permission.READ],
        conditions={"time_range": (9, 17)}  # Business hours only
    )
    
    # Check access
    has_access = security.check_access("data-analyst", "user_events", Permission.READ)
    print(f"Data analyst has read access: {has_access}")
    
    # Auto-scaling
    scaler = AutoScaler()
    scaler.create_scaling_policy(
        resource_id="kafka-cluster",
        min_capacity=3,
        max_capacity=10,
        target_cpu=70.0,
        target_memory=80.0
    )
    
    # Evaluate scaling
    scaling_decision = scaler.evaluate_scaling("kafka-cluster", 85.0, 75.0)
    print(f"Scaling decision: {scaling_decision}")
    
    # Save policies
    security.save_policies()
    print("Security policies saved")
