# Codeforces Contest Submission Analysis

Big Data Architecture

```            
                            ┌──────────────────────────────────────────────┐                           
                            │                        ┌─────┐               │                           
                            │                        │Kafka│               │                           
                            │                        └──┬──┘               │                           
                            │┌───────────────────────┐  │                  │ Bronze                    
                            ││Submissions Fake Stream│  │  ┌─────────────┐ │                           
                            ││(Python script)        ├──▼──►Spark (Clean)│ │                           
                            │└───────────────────────┘     └──────┬──────┘ │                           
                            └─────────────────────────────────────│────────┘                           
                                                                  │                                    
                                  ┌───────────────────────────────┤                                    
                                  │                               │                                    
                                  │                    ┌──────────│────────┐                           
                            ┌─────▼──────┐             │┌─────────▼───────┐│                           
                            │Spark (Flat)│             ││Spark (Transform)││ Silver                    
                            └─────┬──────┘             │└─────────┬───────┘│                           
                    ┌─────┐       │                    └──────────┼────────┘                           
                    │Kafka│───────►                               │                                    
                    └─────┘       │                         ┌─────┼─────┐                              
                            ┌─────▼─────┐                   │┌────▼────┐│                              
                            │Clickhouse │                   ││Deltalake││    Gold                      
                            └─────┬─────┘                   │└────┬────┘│                              
                                  │                         └─────┼─────┘                              
                                  │                               │                                    
                                  │                               │                                    
                                  │                               │                                    
                              ┌───▼───┐                      ┌────▼───┐                                
                              │Grafana│                      │Superset│                                
                              └───────┘                      └────────┘                                
```
