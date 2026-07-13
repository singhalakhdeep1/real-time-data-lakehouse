"""
Data Catalog for Real-time Data Lakehouse
Manages metadata, schema registry, and data discovery
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import hashlib


@dataclass
class Dataset:
    """Represents a dataset in the catalog"""
    id: str
    name: str
    description: str
    format: str  # parquet, json, avro, etc.
    location: str
    schema: Dict[str, str]
    tags: List[str]
    owner: str
    created_at: str
    updated_at: str
    size_bytes: int
    row_count: int


@dataclass
class DataPipeline:
    """Represents a data pipeline in the catalog"""
    id: str
    name: str
    description: str
    source_datasets: List[str]
    target_datasets: List[str]
    schedule: str
    status: str
    owner: str
    created_at: str


class DataCatalog:
    """Central catalog for all data assets"""
    
    def __init__(self):
        self.datasets: Dict[str, Dataset] = {}
        self.pipelines: Dict[str, DataPipeline] = {}
        self.catalog_file = Path("catalog/data_catalog.json")
        self.catalog_file.parent.mkdir(exist_ok=True)
        
    def register_dataset(self, name: str, description: str, format: str, location: str, 
                         schema: Dict[str, str], tags: List[str], owner: str, 
                         size_bytes: int = 0, row_count: int = 0) -> str:
        """Register a new dataset in the catalog"""
        dataset_id = self._generate_id(name)
        
        dataset = Dataset(
            id=dataset_id,
            name=name,
            description=description,
            format=format,
            location=location,
            schema=schema,
            tags=tags,
            owner=owner,
            created_at=datetime.now().isoformat(),
            updated_at=datetime.now().isoformat(),
            size_bytes=size_bytes,
            row_count=row_count
        )
        
        self.datasets[dataset_id] = dataset
        return dataset_id
    
    def register_pipeline(self, name: str, description: str, source_datasets: List[str], 
                         target_datasets: List[str], schedule: str, owner: str) -> str:
        """Register a new data pipeline"""
        pipeline_id = self._generate_id(name)
        
        pipeline = DataPipeline(
            id=pipeline_id,
            name=name,
            description=description,
            source_datasets=source_datasets,
            target_datasets=target_datasets,
            schedule=schedule,
            status="active",
            owner=owner,
            created_at=datetime.now().isoformat()
        )
        
        self.pipelines[pipeline_id] = pipeline
        return pipeline_id
    
    def search_datasets(self, query: str, tags: List[str] = None) -> List[Dataset]:
        """Search datasets by name, description, or tags"""
        results = []
        query_lower = query.lower()
        
        for dataset in self.datasets.values():
            # Text search
            if (query_lower in dataset.name.lower() or 
                query_lower in dataset.description.lower()):
                results.append(dataset)
                continue
            
            # Tag search
            if tags:
                if any(tag in dataset.tags for tag in tags):
                    results.append(dataset)
        
        return results
    
    def get_dataset_lineage(self, dataset_id: str) -> Dict[str, List[Dataset]]:
        """Get upstream and downstream lineage for a dataset"""
        upstream = []
        downstream = []
        
        # Find pipelines that use this dataset as target (upstream)
        for pipeline in self.pipelines.values():
            if dataset_id in pipeline.target_datasets:
                for source_id in pipeline.source_datasets:
                    if source_id in self.datasets:
                        upstream.append(self.datasets[source_id])
        
        # Find pipelines that use this dataset as source (downstream)
        for pipeline in self.pipelines.values():
            if dataset_id in pipeline.source_datasets:
                for target_id in pipeline.target_datasets:
                    if target_id in self.datasets:
                        downstream.append(self.datasets[target_id])
        
        return {
            "upstream": upstream,
            "downstream": downstream
        }
    
    def get_datasets_by_tag(self, tag: str) -> List[Dataset]:
        """Get all datasets with a specific tag"""
        return [d for d in self.datasets.values() if tag in d.tags]
    
    def get_datasets_by_owner(self, owner: str) -> List[Dataset]:
        """Get all datasets owned by a specific user"""
        return [d for d in self.datasets.values() if d.owner == owner]
    
    def update_dataset(self, dataset_id: str, **kwargs):
        """Update dataset metadata"""
        if dataset_id in self.datasets:
            dataset = self.datasets[dataset_id]
            for key, value in kwargs.items():
                if hasattr(dataset, key):
                    setattr(dataset, key, value)
            dataset.updated_at = datetime.now().isoformat()
    
    def delete_dataset(self, dataset_id: str):
        """Delete a dataset from the catalog"""
        if dataset_id in self.datasets:
            del self.datasets[dataset_id]
    
    def save_catalog(self):
        """Save catalog to file"""
        catalog_data = {
            "datasets": [asdict(d) for d in self.datasets.values()],
            "pipelines": [asdict(p) for p in self.pipelines.values()],
            "saved_at": datetime.now().isoformat()
        }
        self.catalog_file.write_text(json.dumps(catalog_data, indent=2))
    
    def load_catalog(self):
        """Load catalog from file"""
        if self.catalog_file.exists():
            catalog_data = json.loads(self.catalog_file.read_text())
            
            # Reconstruct datasets
            for dataset_data in catalog_data["datasets"]:
                dataset = Dataset(**dataset_data)
                self.datasets[dataset.id] = dataset
            
            # Reconstruct pipelines
            for pipeline_data in catalog_data["pipelines"]:
                pipeline = DataPipeline(**pipeline_data)
                self.pipelines[pipeline.id] = pipeline
    
    def get_catalog_summary(self) -> Dict[str, Any]:
        """Get summary of catalog contents"""
        total_size = sum(d.size_bytes for d in self.datasets.values())
        total_rows = sum(d.row_count for d in self.datasets.values())
        
        tag_counts = {}
        for dataset in self.datasets.values():
            for tag in dataset.tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        return {
            "total_datasets": len(self.datasets),
            "total_pipelines": len(self.pipelines),
            "total_size_bytes": total_size,
            "total_rows": total_rows,
            "unique_tags": len(tag_counts),
            "tag_distribution": tag_counts,
            "owners": list(set(d.owner for d in self.datasets.values()))
        }
    
    def _generate_id(self, name: str) -> str:
        """Generate unique ID from name"""
        return hashlib.md5(f"{name}{datetime.now().isoformat()}".encode()).hexdigest()[:16]


class SchemaRegistry:
    """Registry for data schemas"""
    
    def __init__(self):
        self.schemas: Dict[str, Dict] = {}
        self.schema_versions: Dict[str, List[Dict]] = {}
        
    def register_schema(self, schema_name: str, schema: Dict, version: str = "1.0"):
        """Register a new schema"""
        schema_id = f"{schema_name}_{version}"
        
        self.schemas[schema_id] = {
            "name": schema_name,
            "version": version,
            "schema": schema,
            "registered_at": datetime.now().isoformat()
        }
        
        if schema_name not in self.schema_versions:
            self.schema_versions[schema_name] = []
        self.schema_versions[schema_name].append({
            "version": version,
            "schema_id": schema_id,
            "registered_at": datetime.now().isoformat()
        })
        
        return schema_id
    
    def get_schema(self, schema_name: str, version: str = None) -> Optional[Dict]:
        """Get schema by name and version"""
        if version:
            schema_id = f"{schema_name}_{version}"
            return self.schemas.get(schema_id)
        else:
            # Return latest version
            versions = self.schema_versions.get(schema_name, [])
            if versions:
                latest = versions[-1]
                return self.schemas.get(latest["schema_id"])
        return None
    
    def validate_data(self, schema_name: str, data: Dict) -> bool:
        """Validate data against schema"""
        schema = self.get_schema(schema_name)
        if not schema:
            return False
        
        schema_def = schema["schema"]
        
        # Check required fields
        for field, field_type in schema_def.items():
            if field not in data:
                return False
            
            # Type checking (simplified)
            if field_type == "string" and not isinstance(data[field], str):
                return False
            elif field_type == "number" and not isinstance(data[field], (int, float)):
                return False
            elif field_type == "boolean" and not isinstance(data[field], bool):
                return False
        
        return True


if __name__ == "__main__":
    # Example usage
    catalog = DataCatalog()
    
    # Register some datasets
    catalog.register_dataset(
        name="user_events",
        description="User activity events from web and mobile",
        format="parquet",
        location="s3://data-lake/user_events/",
        schema={"user_id": "string", "event_type": "string", "timestamp": "string"},
        tags=["events", "user", "real-time"],
        owner="data-team",
        size_bytes=1024000000,
        row_count=10000000
    )
    
    catalog.register_dataset(
        name="transactions",
        description="Financial transaction records",
        format="parquet",
        location="s3://data-lake/transactions/",
        schema={"transaction_id": "string", "amount": "number", "currency": "string"},
        tags=["finance", "transactions"],
        owner="finance-team",
        size_bytes=512000000,
        row_count=5000000
    )
    
    # Register a pipeline
    catalog.register_pipeline(
        name="daily_user_aggregation",
        description="Aggregates user events to daily metrics",
        source_datasets=[list(catalog.datasets.keys())[0]],
        target_datasets=[],
        schedule="daily",
        owner="data-team"
    )
    
    # Save catalog
    catalog.save_catalog()
    
    # Get summary
    summary = catalog.get_catalog_summary()
    print("Catalog Summary:")
    print(json.dumps(summary, indent=2))
