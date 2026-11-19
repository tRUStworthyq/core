package com.trustworthyq.core.collection.scoring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties("app.aging")
public record AgingRules(int recentDays, int coolingDays, int unusedDays, int auditLagDays,
                         double recentUsage, double coolingUsage, double unusedUsage,
                         int oldYears, long smallBytes, double duplicateWeight, double nameWeight,
                         double folderWeight, double ownerWeight, double oldWeight, double smallWeight,
                         int coolingScore, int obsoleteScore, List<String> nameMarkers, List<String> folderMarkers) {
}
