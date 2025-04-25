from typing import Dict, List, Optional, Tuple, Union

from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig, RecognizerResult

from ..utils.logger import app_logger as logger

class PresidioAnonymizer:
    """PII anonymization using Microsoft Presidio Anonymizer."""
    
    ANONYMIZATION_METHODS = ["replace", "redact", "mask", "hash", "encrypt"]
    
    def __init__(self, default_method: str = "replace"):
        """Initialize Presidio Anonymizer.
        
        Args:
            default_method: Default anonymization method
              (replace, redact, mask, hash, encrypt)
        """
        if default_method not in self.ANONYMIZATION_METHODS:
            raise ValueError(
                f"Invalid anonymization method: {default_method}. "
                f"Must be one of {', '.join(self.ANONYMIZATION_METHODS)}"
            )
            
        self.default_method = default_method
        self.anonymizer = AnonymizerEngine()
        
    def _convert_to_recognizer_results(self, entities: List[Dict]) -> List[RecognizerResult]:
        """Convert entity dictionaries to RecognizerResult objects.
        
        Args:
            entities: List of entity dictionaries
            
        Returns:
            List[RecognizerResult]: List of recognizer results
        """
        results = []
        for entity in entities:
            result = RecognizerResult(
                entity_type=entity["entity_type"],
                start=entity["start"],
                end=entity["end"],
                score=entity["score"]
            )
            results.append(result)
            
        return results
        
    def anonymize_text(self, 
                       text: str, 
                       entities: List[Dict],
                       method: Optional[str] = None,
                       operators: Optional[Dict[str, Dict]] = None) -> Tuple[str, Dict]:
        """Anonymize text using detected entities.
        
        Args:
            text: Text to anonymize
            entities: List of entity dictionaries
            method: Anonymization method (overrides default)
            operators: Custom operators per entity type
            
        Returns:
            Tuple[str, Dict]: Anonymized text and anonymization metadata
        """
        if not text or not text.strip():
            logger.warning("Empty text provided for anonymization")
            return text, {}
            
        if not entities:
            logger.info("No entities to anonymize")
            return text, {}
            
        use_method = method or self.default_method
        
        try:
            # Convert entity dictionaries to RecognizerResult objects
            recognizer_results = self._convert_to_recognizer_results(entities)
            
            # Set up operators
            operator_config = {}
            
            # If custom operators are provided, use them
            if operators:
                for entity_type, params in operators.items():
                    operator_config[entity_type] = OperatorConfig(
                        operator_name=params.get("method", use_method),
                        params=params.get("params", {})
                    )
            # Otherwise use default method for all entities
            else:
                for entity in entities:
                    entity_type = entity["entity_type"]
                    if entity_type not in operator_config:
                        operator_config[entity_type] = OperatorConfig(
                            operator_name=use_method
                        )
            
            # Anonymize text
            result = self.anonymizer.anonymize(
                text=text,
                analyzer_results=recognizer_results,
                operators=operator_config
            )
            
            anonymized_text = result.text
            items = result.items
            
            # Convert items to more user-friendly format
            metadata = {
                "anonymized_count": len(items),
                "details": [
                    {
                        "entity_type": item.entity_type,
                        "start": item.start,
                        "end": item.end,
                        "original_text": text[item.start:item.end],
                        "anonymized_text": anonymized_text[item.start:item.end],
                        "operator": item.operator
                    }
                    for item in items
                ]
            }
            
            logger.info(f"Anonymized {len(items)} entities")
            return anonymized_text, metadata
            
        except Exception as e:
            logger.error(f"Error anonymizing text: {e}")
            return text, {"error": str(e)}
            
    def anonymize_batch(self, 
                        texts: List[str], 
                        batch_entities: List[List[Dict]],
                        method: Optional[str] = None,
                        operators: Optional[Dict[str, Dict]] = None) -> List[Tuple[str, Dict]]:
        """Anonymize batch of texts using detected entities.
        
        Args:
            texts: List of texts to anonymize
            batch_entities: List of entity lists (one per text)
            method: Anonymization method (overrides default)
            operators: Custom operators per entity type
            
        Returns:
            List[Tuple[str, Dict]]: List of anonymized texts and metadata
        """
        if not texts:
            logger.warning("Empty batch provided for anonymization")
            return []
            
        if len(texts) != len(batch_entities):
            logger.error("Mismatched number of texts and entity lists")
            return [(text, {"error": "Mismatched entity list"}) for text in texts]
            
        results = []
        for i, (text, entities) in enumerate(zip(texts, batch_entities)):
            anonymized, metadata = self.anonymize_text(
                text=text,
                entities=entities,
                method=method,
                operators=operators
            )
            results.append((anonymized, metadata))
            
        return results 