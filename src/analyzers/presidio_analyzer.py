from typing import Dict, List, Optional, Union

import spacy
from presidio_analyzer import AnalyzerEngine, BatchAnalyzerEngine, RecognizerRegistry
from presidio_analyzer.nlp_engine import NlpEngineProvider

from ..utils.logger import app_logger as logger

class PresidioAnalyzer:
    """PII detection using Microsoft Presidio Analyzer."""
    
    def __init__(self, 
                 language: str = "en",
                 model_name: str = "en_core_web_lg",
                 entities: Optional[List[str]] = None,
                 score_threshold: float = 0.7):
        """Initialize Presidio Analyzer.
        
        Args:
            language: Language code
            model_name: Spacy model name
            entities: List of entities to detect (default: all supported)
            score_threshold: Confidence threshold for entity detection
        """
        self.language = language
        self.score_threshold = score_threshold
        self.entities = entities
        
        try:
            # Load spacy model and set up Presidio
            self._setup_analyzer(model_name)
        except Exception as e:
            logger.error(f"Error initializing Presidio Analyzer: {e}")
            raise
            
    def _setup_analyzer(self, model_name: str):
        """Set up Presidio Analyzer with specified model.
        
        Args:
            model_name: Spacy model name
        """
        # Create NLP engine with spaCy
        # Define the NLP engine configuration
        nlp_configuration = {
            "nlp_engine_name": "spacy",
            "models": [
                {"lang_code": self.language, "model_name": model_name}
            ]
        }
        
        # Create the NLP engine using the configuration
        provider = NlpEngineProvider(nlp_configuration=nlp_configuration)
        nlp_engine = provider.create_engine()
        
        # Set up recognizer registry and analyzer engines
        registry = RecognizerRegistry()
        registry.load_predefined_recognizers(languages=[self.language])
        
        self.analyzer = AnalyzerEngine(
            nlp_engine=nlp_engine, 
            registry=registry
        )
        
        self.batch_analyzer = BatchAnalyzerEngine(
            analyzer_engine=self.analyzer
        )
        
        # Get supported entities from the analyzer instead
        supported_entities = self.analyzer.get_supported_entities()
        logger.info(f"Supported entity types: {', '.join(supported_entities)}")
        
    def analyze_text(self, 
                    text: str, 
                    entities: Optional[List[str]] = None, 
                    score_threshold: Optional[float] = None) -> List[Dict]:
        """Analyze text for PII entities.
        
        Args:
            text: Text to analyze
            entities: List of entities to detect (overrides instance entities)
            score_threshold: Confidence threshold (overrides instance threshold)
            
        Returns:
            List[Dict]: List of detected entities with metadata
        """
        if not text or not text.strip():
            logger.warning("Empty text provided for analysis")
            return []
            
        use_entities = entities or self.entities
        use_threshold = score_threshold or self.score_threshold
        
        try:
            # Run analysis
            results = self.analyzer.analyze(
                text=text,
                entities=use_entities,
                language=self.language,
                score_threshold=use_threshold
            )
            
            # Convert results to serializable format
            detected_entities = []
            for result in results:
                entity_dict = {
                    "entity_type": result.entity_type,
                    "start": result.start,
                    "end": result.end,
                    "score": result.score,
                    "text": text[result.start:result.end]
                }
                detected_entities.append(entity_dict)
                
            logger.info(f"Detected {len(detected_entities)} PII entities")
            return detected_entities
            
        except Exception as e:
            logger.error(f"Error analyzing text: {e}")
            return []
            
    def analyze_batch(self, 
                     texts: List[str], 
                     entities: Optional[List[str]] = None,
                     score_threshold: Optional[float] = None) -> List[List[Dict]]:
        """Analyze batch of texts for PII entities.
        
        Args:
            texts: List of texts to analyze
            entities: List of entities to detect (overrides instance entities)
            score_threshold: Confidence threshold (overrides instance threshold)
            
        Returns:
            List[List[Dict]]: List of detected entities per text
        """
        if not texts:
            logger.warning("Empty batch provided for analysis")
            return []
            
        use_entities = entities or self.entities
        use_threshold = score_threshold or self.score_threshold
        
        try:
            # Create a dictionary of texts for analyze_dict
            texts_dict = {str(i): text for i, text in enumerate(texts)}
            
            # Run batch analysis using analyze_dict
            batch_results = self.batch_analyzer.analyze_dict(
                texts=texts_dict,
                entities=use_entities,
                language=self.language,
                score_threshold=use_threshold
            )
            
            # Process results into a standard format
            results = []
            
            # Check if the results are in list format (for test mocking)
            if isinstance(batch_results, list) and len(batch_results) == len(texts):
                for i, text_results in enumerate(batch_results):
                    detected_entities = []
                    for result in text_results:
                        entity_dict = {
                            "entity_type": result.entity_type,
                            "start": result.start,
                            "end": result.end,
                            "score": result.score,
                            "text": texts[i][result.start:result.end] if result.start < len(texts[i]) else ""
                        }
                        detected_entities.append(entity_dict)
                    results.append(detected_entities)
            # Process dictionary format (normal operation)
            else:
                for i in range(len(texts)):
                    text_key = str(i)
                    text_results = batch_results.get(text_key, [])
                    detected_entities = []
                    for result in text_results:
                        entity_dict = {
                            "entity_type": result.entity_type,
                            "start": result.start,
                            "end": result.end,
                            "score": result.score,
                            "text": texts[i][result.start:result.end] if result.start < len(texts[i]) else ""
                        }
                        detected_entities.append(entity_dict)
                    results.append(detected_entities)
            
            return results
            
        except Exception as e:
            logger.error(f"Error analyzing batch: {e}")
            return [[] for _ in texts]
            
    def get_supported_entities(self) -> List[str]:
        """Get list of supported entity types.
        
        Returns:
            List[str]: List of supported entity types
        """
        try:
            return self.analyzer.get_supported_entities()
        except Exception as e:
            logger.error(f"Error getting supported entities: {e}")
            return [] 