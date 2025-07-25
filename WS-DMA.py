# WebSocket Integration for Data Mapping Workflow
# Add these imports to your existing imports section

import asyncio
import websockets
import json
from datetime import datetime
from typing import Dict, Any, Callable, Optional
import threading
import queue
import uuid

# --- WebSocket Server Class ---
class DataMappingWebSocketServer:
    def __init__(self, host="localhost", port=8765):
        self.host = host
        self.port = port
        self.clients = set()
        self.current_session = None
        self.workflow_state = {}
        self.user_input_queue = queue.Queue()
        self.user_response_event = threading.Event()
        self.user_response = None
        
    async def register_client(self, websocket):
        """Register a new client connection"""
        self.clients.add(websocket)
        await self.send_message(websocket, {
            "type": "connection_established",
            "message": "Connected to Data Mapping Workflow",
            "timestamp": datetime.now().isoformat()
        })
        print(f"Client connected. Total clients: {len(self.clients)}")
    
    async def unregister_client(self, websocket):
        """Unregister a client connection"""
        self.clients.discard(websocket)
        print(f"Client disconnected. Total clients: {len(self.clients)}")
    
    async def send_message(self, websocket, message):
        """Send message to a specific client"""
        try:
            await websocket.send(json.dumps(message))
        except websockets.exceptions.ConnectionClosed:
            await self.unregister_client(websocket)
    
    async def broadcast_message(self, message):
        """Broadcast message to all connected clients"""
        if self.clients:
            disconnected = []
            for client in self.clients.copy():
                try:
                    await client.send(json.dumps(message))
                except websockets.exceptions.ConnectionClosed:
                    disconnected.append(client)
            
            for client in disconnected:
                await self.unregister_client(client)
    
    async def handle_client_message(self, websocket, message_data):
        """Handle incoming messages from clients"""
        try:
            message = json.loads(message_data)
            message_type = message.get("type")
            
            if message_type == "start_workflow":
                await self.start_workflow_session(websocket, message.get("config", {}))
            
            elif message_type == "user_input":
                # Handle user input for HITL interactions
                self.user_response = message.get("data")
                self.user_response_event.set()
            
            elif message_type == "get_workflow_status":
                await self.send_workflow_status(websocket)
            
            else:
                await self.send_message(websocket, {
                    "type": "error",
                    "message": f"Unknown message type: {message_type}"
                })
                
        except json.JSONDecodeError:
            await self.send_message(websocket, {
                "type": "error",
                "message": "Invalid JSON format"
            })
        except Exception as e:
            await self.send_message(websocket, {
                "type": "error",
                "message": f"Error processing message: {str(e)}"
            })
    
    async def start_workflow_session(self, websocket, config):
        """Start a new workflow session"""
        session_id = str(uuid.uuid4())
        self.current_session = session_id
        
        await self.broadcast_message({
            "type": "workflow_started",
            "session_id": session_id,
            "message": "Data mapping workflow started",
            "timestamp": datetime.now().isoformat()
        })
        
        # Start workflow in a separate thread to avoid blocking
        workflow_thread = threading.Thread(
            target=self.run_workflow_thread,
            args=(session_id, config),
            daemon=True
        )
        workflow_thread.start()
    
    def run_workflow_thread(self, session_id, config):
        """Run the workflow in a separate thread"""
        try:
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Run the workflow with WebSocket integration
            self.execute_workflow_with_websocket(session_id, config)
            
        except Exception as e:
            # Send error to all clients
            asyncio.create_task(self.broadcast_message({
                "type": "workflow_error",
                "session_id": session_id,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }))
    
    def execute_workflow_with_websocket(self, session_id, config):
        """Execute the workflow with WebSocket notifications"""
        # This will be called from the main workflow execution
        # The actual workflow execution happens in the main thread
        pass
    
    async def send_workflow_status(self, websocket):
        """Send current workflow status to client"""
        await self.send_message(websocket, {
            "type": "workflow_status",
            "session_id": self.current_session,
            "state": self.workflow_state,
            "timestamp": datetime.now().isoformat()
        })
    
    async def request_user_input(self, input_type, data):
        """Request user input for HITL interactions"""
        # Reset the response event
        self.user_response_event.clear()
        self.user_response = None
        
        # Send request to all clients
        await self.broadcast_message({
            "type": "user_input_required",
            "input_type": input_type,
            "data": data,
            "timestamp": datetime.now().isoformat()
        })
        
        # Wait for user response (with timeout)
        response_received = self.user_response_event.wait(timeout=300)  # 5 minutes timeout
        
        if response_received:
            return self.user_response
        else:
            raise TimeoutError("User input timeout after 5 minutes")
    
    async def handle_connection(self, websocket, path):
        """Handle WebSocket connections"""
        await self.register_client(websocket)
        try:
            async for message in websocket:
                await self.handle_client_message(websocket, message)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await self.unregister_client(websocket)
    
    def start_server(self):
        """Start the WebSocket server"""
        print(f"Starting WebSocket server on {self.host}:{self.port}")
        return websockets.serve(self.handle_connection, self.host, self.port)

# --- Global WebSocket server instance ---
ws_server = DataMappingWebSocketServer()

# --- Modified HITL Helper Functions ---
def print_mapping_suggestion_for_review_ws(suggestion: MappingSuggestion, index: int, total: int, app_dict_map: Dict):
    """Prepare mapping suggestion data for WebSocket transmission"""
    app_attr = suggestion.application_attribute
    details = app_dict_map.get(app_attr, {})
    
    return {
        "suggestion_index": index + 1,
        "total_suggestions": total,
        "application_attribute": {
            "name": app_attr,
            "description": details.get('Description', 'N/A'),
            "data_type": details.get('DataType', 'N/A')
        },
        "llm_suggestion": {
            "mapping_type": suggestion.mapping_type,
            "cdm_entity": suggestion.cdm_entity,
            "cdm_attribute": suggestion.cdm_attribute,
            "confidence_score": suggestion.confidence_score,
            "reasoning": suggestion.reasoning,
            "semantic_candidates": suggestion.semantic_candidates_considered[:5] if suggestion.semantic_candidates_considered else []
        }
    }

async def get_user_mapping_decision_ws(suggestion_data):
    """Get user's decision via WebSocket"""
    response = await ws_server.request_user_input("mapping_decision", suggestion_data)
    return response.get("action", "R")  # Default to reject if no action provided

async def get_user_input_ws(input_type, prompt, current_value=None, choices=None, required_type=str):
    """Get user input via WebSocket"""
    request_data = {
        "prompt": prompt,
        "current_value": current_value,
        "choices": choices,
        "required_type": required_type.__name__ if required_type else "str"
    }
    
    response = await ws_server.request_user_input(input_type, request_data)
    
    if required_type == float:
        try:
            return float(response.get("value", 0.0))
        except (ValueError, TypeError):
            return 0.0
    
    return response.get("value", "")

# --- Modified HITL Node Functions ---
async def human_review_mappings_ws(state: DataMappingState) -> DataMappingState:
    """WebSocket-enabled version of human_review_mappings"""
    logging.info(f"--- Node: human_review_mappings (WebSocket) ---")
    log = state.get('analysis_log', [])
    suggestions_to_review = state.get('current_batch_suggestions_llm', [])
    validated_suggestions = state.get('validated_mapping_suggestions', [])
    app_dict_map = state.get('app_dict_map', {})
    
    # Notify frontend that review is starting
    await ws_server.broadcast_message({
        "type": "review_phase_started",
        "phase": "mapping_suggestions",
        "total_items": len(suggestions_to_review),
        "timestamp": datetime.now().isoformat()
    })

    if not suggestions_to_review:
        log.append("No suggestions in the current batch to review.")
        idx = state.get('current_attribute_index', 0)
        app_dict = state.get('app_dict_list', [])
        batch_size = min(BATCH_SIZE, len(app_dict) - idx)
        state['current_attribute_index'] = idx + batch_size
        log.append(f"Advanced index to {state['current_attribute_index']}")
        state['analysis_log'] = log
        
        await ws_server.broadcast_message({
            "type": "review_phase_completed",
            "phase": "mapping_suggestions",
            "message": "No suggestions to review in this batch"
        })
        return state

    log.append("Starting WebSocket-based human review for mapping suggestions batch...")
    reviewed_batch_suggestions = []
    processed_attr_names_this_batch = []

    for i, suggestion in enumerate(suggestions_to_review):
        if not isinstance(suggestion, MappingSuggestion):
            log.append(f"Warning: Skipping invalid item in review batch: {type(suggestion).__name__}")
            continue

        processed_attr_names_this_batch.append(suggestion.application_attribute)
        
        # Prepare suggestion data for frontend
        suggestion_data = print_mapping_suggestion_for_review_ws(suggestion, i, len(suggestions_to_review), app_dict_map)
        
        while True:  # Loop for modifications
            try:
                # Send current suggestion to frontend and wait for decision
                action_response = await ws_server.request_user_input("mapping_decision", {
                    **suggestion_data,
                    "current_suggestion": {
                        "mapping_type": suggestion.mapping_type,
                        "cdm_entity": suggestion.cdm_entity,
                        "cdm_attribute": suggestion.cdm_attribute,
                        "confidence_score": suggestion.confidence_score,
                        "reasoning": suggestion.reasoning
                    }
                })
                
                action = action_response.get("action", "R")
                
                if action == 'A':  # Accept
                    log.append(f"Reviewer Accepted: '{suggestion.application_attribute}' as {suggestion.mapping_type}")
                    reviewed_batch_suggestions.append(suggestion)
                    break
                
                elif action == 'R':  # Reject
                    log.append(f"Reviewer Rejected: '{suggestion.application_attribute}'. Recording as NO_MATCH.")
                    suggestion.mapping_type = 'NO_MATCH'
                    suggestion.cdm_entity = None
                    suggestion.cdm_attribute = None
                    suggestion.confidence_score = None
                    suggestion.reasoning += " [Rejected by human reviewer]"
                    reviewed_batch_suggestions.append(suggestion)
                    break
                
                elif action == 'M':  # Modify
                    modifications = action_response.get("modifications", {})
                    
                    if "mapping_type" in modifications:
                        new_type = modifications["mapping_type"]
                        suggestion.mapping_type = new_type
                        if new_type != 'EXISTING':
                            suggestion.confidence_score = None
                        if new_type not in ['EXISTING', 'NEW_ATTRIBUTE']:
                            suggestion.cdm_entity = None
                        if new_type != 'EXISTING':
                            suggestion.cdm_attribute = None
                        suggestion.reasoning += f" [Type modified to {new_type} by reviewer]"
                    
                    if "cdm_entity" in modifications:
                        suggestion.cdm_entity = modifications["cdm_entity"]
                        suggestion.reasoning += f" [Entity modified by reviewer]"
                    
                    if "cdm_attribute" in modifications:
                        suggestion.cdm_attribute = modifications["cdm_attribute"]
                        suggestion.reasoning += f" [Attribute modified by reviewer]"
                    
                    if "confidence_score" in modifications:
                        suggestion.confidence_score = float(modifications["confidence_score"])
                        suggestion.reasoning += f" [Confidence modified by reviewer]"
                    
                    log.append(f"Reviewer Modified '{suggestion.application_attribute}'")
                    # Continue loop to show updated suggestion
                
            except Exception as e:
                log.append(f"Error during WebSocket user input: {e}")
                # Default to reject on error
                suggestion.mapping_type = 'NO_MATCH'
                suggestion.reasoning += f" [Auto-rejected due to input error: {e}]"
                reviewed_batch_suggestions.append(suggestion)
                break

    # Post-review validation and state update (same as original)
    final_validated_this_batch = []
    log.append("--- Performing Post-Review Validation ---")
    
    for suggestion in reviewed_batch_suggestions:
        # ... (same validation logic as original)
        final_validated_this_batch.append(suggestion)

    # Update state
    validated_suggestions.extend(final_validated_this_batch)
    state['validated_mapping_suggestions'] = validated_suggestions
    state['current_batch_suggestions_llm'] = []
    
    # Advance index
    idx = state.get('current_attribute_index', 0)
    batch_actual_size = len(suggestions_to_review)
    state['current_attribute_index'] = idx + batch_actual_size
    state['processed_attributes'].extend(processed_attr_names_this_batch)

    log.append(f"WebSocket human review completed for batch. Added {len(final_validated_this_batch)} suggestions to validated list.")
    state['analysis_log'] = log
    
    # Notify frontend that review phase is complete
    await ws_server.broadcast_message({
        "type": "review_phase_completed",
        "phase": "mapping_suggestions",
        "validated_count": len(final_validated_this_batch),
        "total_validated": len(validated_suggestions)
    })
    
    return state

async def human_review_proposals_ws(state: DataMappingState) -> DataMappingState:
    """WebSocket-enabled version of human_review_proposals"""
    logging.info(f"--- Node: human_review_proposals (WebSocket) ---")
    log = state.get('analysis_log', [])
    term_proposals_to_review = state.get('current_term_proposals_llm', [])
    entity_proposals_to_review = state.get('current_entity_proposals_llm', [])

    final_term_requests = []
    final_entity_requests = []

    # Notify frontend that proposal review is starting
    await ws_server.broadcast_message({
        "type": "review_phase_started",
        "phase": "proposals",
        "term_count": len(term_proposals_to_review),
        "entity_count": len(entity_proposals_to_review),
        "timestamp": datetime.now().isoformat()
    })

    # Review Term Proposals
    if term_proposals_to_review:
        log.append(f"Starting WebSocket review for {len(term_proposals_to_review)} term proposals...")
        
        for i, term in enumerate(term_proposals_to_review):
            if not isinstance(term, NewTermSuggestion):
                continue

            term_data = {
                "proposal_index": i + 1,
                "total_proposals": len(term_proposals_to_review),
                "type": "term",
                "application_attribute": term.application_attribute,
                "target_cdm_entity": term.target_cdm_entity,
                "suggested_term_name": term.suggested_term_name,
                "suggested_data_type": term.suggested_data_type,
                "suggested_definition": term.suggested_definition,
                "reasoning": term.reasoning
            }
            
            try:
                response = await ws_server.request_user_input("proposal_decision", term_data)
                action = response.get("action", "R")
                
                if action == 'A':  # Accept
                    log.append(f"Reviewer Accepted Term Proposal: '{term.suggested_term_name}'")
                    final_term_requests.append(term)
                
                elif action == 'M':  # Modify
                    modifications = response.get("modifications", {})
                    if "suggested_term_name" in modifications:
                        term.suggested_term_name = modifications["suggested_term_name"]
                    if "suggested_data_type" in modifications:
                        term.suggested_data_type = modifications["suggested_data_type"]
                    if "suggested_definition" in modifications:
                        term.suggested_definition = modifications["suggested_definition"]
                    
                    term.reasoning += " [Modified by reviewer]"
                    log.append(f"Reviewer Modified and Accepted Term Proposal")
                    final_term_requests.append(term)
                
                # 'R' (Reject) - simply don't add to final list
            
            except Exception as e:
                log.append(f"Error during term proposal review: {e}")

    # Review Entity Proposals (similar structure)
    if entity_proposals_to_review:
        log.append(f"Starting WebSocket review for {len(entity_proposals_to_review)} entity proposals...")
        
        for i, entity in enumerate(entity_proposals_to_review):
            if not isinstance(entity, NewEntitySuggestion):
                continue

            entity_data = {
                "proposal_index": i + 1,
                "total_proposals": len(entity_proposals_to_review),
                "type": "entity",
                "suggested_entity_name": entity.suggested_entity_name,
                "driving_application_attributes": entity.driving_application_attributes,
                "suggested_purpose": entity.suggested_purpose,
                "suggested_attributes": entity.suggested_attributes[:10],  # Limit for UI
                "potential_relationships": entity.potential_relationships,
                "reasoning": entity.reasoning
            }
            
            try:
                response = await ws_server.request_user_input("proposal_decision", entity_data)
                action = response.get("action", "R")
                
                if action == 'A':  # Accept
                    log.append(f"Reviewer Accepted Entity Proposal: '{entity.suggested_entity_name}'")
                    final_entity_requests.append(entity)
                
                elif action == 'M':  # Modify
                    modifications = response.get("modifications", {})
                    if "suggested_entity_name" in modifications:
                        entity.suggested_entity_name = modifications["suggested_entity_name"]
                    if "suggested_purpose" in modifications:
                        entity.suggested_purpose = modifications["suggested_purpose"]
                    
                    entity.reasoning += " [Modified by reviewer]"
                    log.append(f"Reviewer Modified and Accepted Entity Proposal")
                    final_entity_requests.append(entity)
            
            except Exception as e:
                log.append(f"Error during entity proposal review: {e}")

    # Update state
    state['new_term_requests'] = final_term_requests
    state['new_entity_requests'] = final_entity_requests
    state['current_term_proposals_llm'] = []
    state['current_entity_proposals_llm'] = []

    log.append(f"WebSocket proposal review complete. Terms: {len(final_term_requests)}, Entities: {len(final_entity_requests)}")
    state['analysis_log'] = log
    
    # Notify frontend
    await ws_server.broadcast_message({
        "type": "review_phase_completed",
        "phase": "proposals",
        "approved_terms": len(final_term_requests),
        "approved_entities": len(final_entity_requests)
    })
    
    return state

# --- Modified Workflow Execution ---
async def execute_workflow_with_websocket():
    """Execute the workflow with WebSocket integration"""
    logging.info("Starting Data Mapping Workflow with WebSocket Integration...")
    
    # Notify clients that workflow is starting
    await ws_server.broadcast_message({
        "type": "workflow_progress",
        "stage": "initialization",
        "message": "Starting workflow execution...",
        "timestamp": datetime.now().isoformat()
    })
    
    try:
        # Create workflow with WebSocket-enabled HITL nodes
        workflow_ws = StateGraph(DataMappingState)
        
        # Add nodes (replace HITL nodes with WebSocket versions)
        workflow_ws.add_node("load_inputs", load_inputs)
        workflow_ws.add_node("setup_vector_store", setup_vector_store)
        workflow_ws.add_node("suggest_mappings_llm", suggest_attribute_mapping_batch)
        workflow_ws.add_node("human_review_mappings", human_review_mappings_ws)  # WebSocket version
        workflow_ws.add_node("consolidate", consolidate_suggestions)
        workflow_ws.add_node("propose_new_llm", propose_new_cdm_elements)
        workflow_ws.add_node("human_review_proposals", human_review_proposals_ws)  # WebSocket version
        workflow_ws.add_node("generate_model", generate_logical_model)
        
        # Same edges as original
        workflow_ws.set_entry_point("load_inputs")
        workflow_ws.add_edge("load_inputs", "setup_vector_store")
        workflow_ws.add_edge("setup_vector_store", "suggest_mappings_llm")
        workflow_ws.add_edge("suggest_mappings_llm", "human_review_mappings")
        
        workflow_ws.add_conditional_edges(
            "human_review_mappings",
            should_continue_mapping,
            {
                "continue_mapping": "suggest_mappings_llm",
                "consolidate": "consolidate"
            }
        )
        
        workflow_ws.add_conditional_edges(
            "consolidate",
            should_propose_new_elements,
            {
                "propose_new": "propose_new_llm",
                "generate_model": "generate_model"
            }
        )
        
        workflow_ws.add_edge("propose_new_llm", "human_review_proposals")
        workflow_ws.add_edge("human_review_proposals", "generate_model")
        workflow_ws.add_edge("generate_model", END)
        
        # Compile and execute
        app_ws = workflow_ws.compile()
        
        await ws_server.broadcast_message({
            "type": "workflow_progress",
            "stage": "execution",
            "message": "Workflow compiled successfully, starting execution...",
        })
        
        # Execute workflow
        final_state = app_ws.invoke({}, {"recursion_limit": 300})
        
        # Send final results to all clients
        await ws_server.broadcast_message({
            "type": "workflow_completed",
            "results": {
                "validated_mappings": len(final_state.get('validated_mapping_suggestions', [])),
                "approved_terms": len(final_state.get('new_term_requests', [])),
                "approved_entities": len(final_state.get('new_entity_requests', [])),
                "has_logical_model": final_state.get('logical_model') is not None
            },
            "timestamp": datetime.now().isoformat()
        })
        
        return final_state
        
    except Exception as e:
        await ws_server.broadcast_message({
            "type": "workflow_error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        })
        raise

# --- Main execution with WebSocket server ---
async def main():
    """Main function to run WebSocket server and workflow"""
    # Start WebSocket server
    server = await ws_server.start_server()
    print(f"WebSocket server started on ws://{ws_server.host}:{ws_server.port}")
    
    try:
        # Keep server running
        await server.wait_closed()
    except KeyboardInterrupt:
        print("Shutting down WebSocket server...")
        server.close()
        await server.wait_closed()

if __name__ == "__main__":
    # Replace the original workflow execution with WebSocket version
    print("Starting Data Mapping Workflow with WebSocket Integration...")
    
    # Run the WebSocket server
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Application stopped by user.")
