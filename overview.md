# **MonFin: Fraud Detection & Risk Scoring System**

## **1. Introduction**
MonFin is a **screening tool** developed by FGP Limburg to detect fraudulent companies and financial crimes. It aggregates data from various **law enforcement and financial sources** to assign risk scores to businesses based on predefined indicators.

## **2. Purpose of MonFin**
- **Identify & prioritize fraudulent companies** using an automated risk scoring system.
- **Centralize law enforcement data** to improve efficiency in financial crime investigations.
- **Provide a proactive approach** to detect criminal activity before it escalates.

## **3. Data Sources Used in MonFin**
MonFin integrates multiple **public and restricted databases** to evaluate companies and their directors.

### **📌 Primary Data Sources**
| **Source** | **Purpose** |
|-----------|------------|
| **Kruispuntbank van Ondernemingen (KBO)** | Identifies company registration details, directors, and past bankruptcies. |
| **Nationale Bank van België (NBB)** | Provides financial data, solvency, and profitability insights. |
| **Algemene Nationale Gegevensbank (ANG)** | Contains criminal records related to economic and organized crime. |
| **Centraal Register van Bestuursverboden (JustBan)** | Tracks individuals banned from holding managerial roles. |
| **Reftab** | Reference tables for cross-checking ANG data. |

### **🚀 Future Data Sources**
| **Source** | **Planned Use** |
|-----------|--------------|
| **DOLSIS (Social Security Database)** | Verifies employment legitimacy within companies. |
| **Rijksregister (National Registry)** | Checks legal addresses of company directors. |
| **DATACROSS III** | Links business owners to international fraud networks. |

## **4. Risk Scoring Mechanism**
MonFin assigns **risk scores** to companies based on weighted **fraud indicators**.

### **🔍 Company-Level Indicators**
| **Indicator** | **Why It’s Risky** |
|--------------|----------------|
| **Frequent management changes** | May indicate fraudulent ownership transfers. |
| **Shared address with flagged companies** | Common in shell company networks. |
| **No physical office or only a virtual address** | Suggests a potentially fake business. |
| **Past bankruptcies** | Possible financial instability or fraud. |
| **Industry with high fraud risks** | Some sectors have a history of economic crimes. |

### **👤 Director-Level Indicators**
| **Indicator** | **Why It’s Risky** |
|--------------|----------------|
| **Past bankruptcies** | Suggests financial mismanagement or fraud. |
| **Listed in JustBan (professional ban)** | Illegal to manage a company under such restrictions. |
| **Criminal record in ANG** | Links to financial or organized crime. |
| **Multiple flagged company connections** | Potential involvement in fraud rings. |

### **📊 Score Calculation Example**
| **Indicator** | **Weight** | **Score** | **Total Contribution** |
|--------------|----------|------|------------------|
| Director has a criminal record (ANG) | **10** | **8** | **80** |
| Frequent management changes | **7** | **6** | **42** |
| No physical office | **5** | **8** | **40** |
| Past bankruptcies | **9** | **7** | **63** |
| **Total Risk Score** | **—** | **—** | **225 (High Risk!)** |

## **5. Investigation & Dashboard Integration**
- **Companies exceeding risk thresholds** are flagged for further police investigation.
- **Power BI dashboard** allows filtering based on fraud indicators.
- Investigators must perform **manual verification** before any legal action is taken.

## **6. Future Enhancements**
✅ **Dynamic Weighting:** Adjust indicator importance based on real-world fraud patterns.
✅ **AI & Machine Learning:** Detect new fraud patterns automatically.
✅ **International Data Cross-Checking:** Use global sources like **DATACROSS III**.

## **7. Implementation Roadmap**
| **Phase** | **Description** | **Completion** |
|----------|----------------|--------------|
| **Phase 1** | Development & Testing (FGP Limburg, Halle-Vilvoorde) | **2024 - Early 2025** |
| **Phase 2** | Nationwide Deployment | **Mid-2025** |
| **Phase 3** | Full Integration into Daily Operations | **Late 2025** |

## **8. Conclusion**
MonFin is a **strategic national initiative** to improve fraud detection and financial crime investigations. By integrating **exclusive law enforcement data**, automating risk scoring, and enabling **real-time monitoring**, it helps police focus on the **most suspicious companies** while ensuring compliance with data protection laws.
