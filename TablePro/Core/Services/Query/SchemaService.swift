//
//  SchemaService.swift
//  TablePro
//

import Combine
import Foundation
import os

@MainActor
@Observable
final class SchemaService {
    static let shared = SchemaService()

    private(set) var states: [UUID: SchemaState] = [:]
    private(set) var procedures: [UUID: [RoutineInfo]] = [:]
    private(set) var functions: [UUID: [RoutineInfo]] = [:]
    private(set) var schemasInOrder: [UUID: [String]] = [:]

    @ObservationIgnored private var lastLoadDates: [UUID: Date] = [:]
    @ObservationIgnored private let loadDedup = OnceTask<UUID, [TableInfo]>()
    @ObservationIgnored private let procedureDedup = OnceTask<UUID, [RoutineInfo]>()
    @ObservationIgnored private let functionDedup = OnceTask<UUID, [RoutineInfo]>()
    @ObservationIgnored private let schemasDedup = OnceTask<UUID, [String]>()
    @ObservationIgnored private var settingsCancellable: AnyCancellable?
    @ObservationIgnored private var lastDisplaySchemasInSidebar: Bool = false
    @ObservationIgnored private static let logger = Logger(subsystem: "com.TablePro", category: "SchemaService")

    init() {
        lastDisplaySchemasInSidebar = AppSettingsManager.shared.editor.displaySchemasInSidebar
        settingsCancellable = AppEvents.shared.editorSettingsChanged
            .sink { [weak self] in
                Task { @MainActor [weak self] in
                    self?.handleEditorSettingsChange()
                }
            }
    }

    func state(for connectionId: UUID) -> SchemaState {
        states[connectionId] ?? .idle
    }

    func tables(for connectionId: UUID) -> [TableInfo] {
        if case .loaded(let tables) = state(for: connectionId) {
            return tables
        }
        return []
    }

    func procedures(for connectionId: UUID) -> [RoutineInfo] {
        procedures[connectionId] ?? []
    }

    func functions(for connectionId: UUID) -> [RoutineInfo] {
        functions[connectionId] ?? []
    }

    func routines(for connectionId: UUID) -> [RoutineInfo] {
        procedures(for: connectionId) + functions(for: connectionId)
    }

    func schemas(for connectionId: UUID) -> [String] {
        schemasInOrder[connectionId] ?? []
    }

    func load(connectionId: UUID, driver: DatabaseDriver, connection: DatabaseConnection) async {
        switch state(for: connectionId) {
        case .loaded:
            return
        case .idle, .loading, .failed:
            await runLoad(connectionId: connectionId, driver: driver, connection: connection)
        }
    }

    func reload(connectionId: UUID, driver: DatabaseDriver, connection: DatabaseConnection) async {
        await runLoad(connectionId: connectionId, driver: driver, connection: connection)
    }

    func reloadIfStale(
        connectionId: UUID,
        driver: DatabaseDriver,
        connection: DatabaseConnection,
        staleness: TimeInterval
    ) async {
        guard let lastLoad = lastLoadDates[connectionId] else {
            await reload(connectionId: connectionId, driver: driver, connection: connection)
            return
        }
        guard Date().timeIntervalSince(lastLoad) > staleness else { return }
        await reload(connectionId: connectionId, driver: driver, connection: connection)
    }

    func reloadProcedures(connectionId: UUID, driver: DatabaseDriver) async {
        let visibleSchemas = visibleSchemasForGroupedReload(connectionId: connectionId, driver: driver)
        do {
            let routines = try await procedureDedup.execute(key: connectionId) {
                if let schemas = visibleSchemas {
                    return try await Self.fetchRoutinesAcrossSchemas(driver: driver, schemas: schemas, kind: .procedure)
                }
                return try await driver.fetchProcedures(schema: nil)
            }
            procedures[connectionId] = routines
        } catch is CancellationError {
            return
        } catch {
            Self.logger.warning(
                "[schema] procedures reload failed connId=\(connectionId, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
            )
        }
    }

    func reloadFunctions(connectionId: UUID, driver: DatabaseDriver) async {
        let visibleSchemas = visibleSchemasForGroupedReload(connectionId: connectionId, driver: driver)
        do {
            let routines = try await functionDedup.execute(key: connectionId) {
                if let schemas = visibleSchemas {
                    return try await Self.fetchRoutinesAcrossSchemas(driver: driver, schemas: schemas, kind: .function)
                }
                return try await driver.fetchFunctions(schema: nil)
            }
            functions[connectionId] = routines
        } catch is CancellationError {
            return
        } catch {
            Self.logger.warning(
                "[schema] functions reload failed connId=\(connectionId, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
            )
        }
    }

    func invalidate(connectionId: UUID) async {
        await loadDedup.cancel(key: connectionId)
        await procedureDedup.cancel(key: connectionId)
        await functionDedup.cancel(key: connectionId)
        await schemasDedup.cancel(key: connectionId)
        states.removeValue(forKey: connectionId)
        procedures.removeValue(forKey: connectionId)
        functions.removeValue(forKey: connectionId)
        schemasInOrder.removeValue(forKey: connectionId)
        lastLoadDates.removeValue(forKey: connectionId)
    }

    private func runLoad(
        connectionId: UUID,
        driver: DatabaseDriver,
        connection: DatabaseConnection
    ) async {
        states[connectionId] = .loading

        let wantsGrouping = AppSettingsManager.shared.editor.displaySchemasInSidebar
            && PluginManager.shared.supportsSchemaSwitching(for: connection.type)

        if wantsGrouping {
            await runSchemaGroupedLoad(connectionId: connectionId, driver: driver, connection: connection)
        } else {
            await runFlatLoad(connectionId: connectionId, driver: driver, connection: connection)
        }
    }

    private func runFlatLoad(
        connectionId: UUID,
        driver: DatabaseDriver,
        connection: DatabaseConnection
    ) async {
        schemasInOrder.removeValue(forKey: connectionId)

        async let tablesTask: [TableInfo] = loadDedup.execute(key: connectionId) {
            try await driver.fetchTables()
        }
        async let proceduresTask: [RoutineInfo] = Self.fetchRoutinesSafely(
            connectionId: connectionId,
            kind: .procedure,
            dedup: procedureDedup,
            fetch: { try await driver.fetchProcedures(schema: nil) }
        )
        async let functionsTask: [RoutineInfo] = Self.fetchRoutinesSafely(
            connectionId: connectionId,
            kind: .function,
            dedup: functionDedup,
            fetch: { try await driver.fetchFunctions(schema: nil) }
        )

        let loadedProcedures = await proceduresTask
        let loadedFunctions = await functionsTask

        do {
            let tables = try await tablesTask
            states[connectionId] = .loaded(tables)
            procedures[connectionId] = loadedProcedures
            functions[connectionId] = loadedFunctions
            lastLoadDates[connectionId] = Date()
        } catch is CancellationError {
            return
        } catch {
            Self.logger.warning(
                "[schema] load failed connId=\(connectionId, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
            )
            states[connectionId] = .failed(error.localizedDescription)
        }
    }

    private func runSchemaGroupedLoad(
        connectionId: UUID,
        driver: DatabaseDriver,
        connection: DatabaseConnection
    ) async {
        let dbType = connection.type
        let allSchemas: [String]
        do {
            allSchemas = try await schemasDedup.execute(key: connectionId) {
                try await driver.fetchSchemas()
            }
        } catch is CancellationError {
            return
        } catch {
            Self.logger.warning(
                "[schema] fetchSchemas failed connId=\(connectionId, privacy: .public) error=\(error.localizedDescription, privacy: .public); falling back to flat load"
            )
            await runFlatLoad(connectionId: connectionId, driver: driver, connection: connection)
            return
        }

        let systemSchemas = Set(PluginManager.shared.systemSchemaNames(for: dbType))
        let visibleSchemas = allSchemas.filter { !systemSchemas.contains($0) }
        schemasInOrder[connectionId] = visibleSchemas

        async let tablesTask: [TableInfo] = loadDedup.execute(key: connectionId) {
            try await Self.fetchTablesAcrossSchemas(driver: driver, schemas: visibleSchemas)
        }
        async let proceduresTask: [RoutineInfo] = Self.fetchRoutinesSafely(
            connectionId: connectionId,
            kind: .procedure,
            dedup: procedureDedup,
            fetch: { try await Self.fetchRoutinesAcrossSchemas(driver: driver, schemas: visibleSchemas, kind: .procedure) }
        )
        async let functionsTask: [RoutineInfo] = Self.fetchRoutinesSafely(
            connectionId: connectionId,
            kind: .function,
            dedup: functionDedup,
            fetch: { try await Self.fetchRoutinesAcrossSchemas(driver: driver, schemas: visibleSchemas, kind: .function) }
        )

        let loadedProcedures = await proceduresTask
        let loadedFunctions = await functionsTask

        do {
            let tables = try await tablesTask
            states[connectionId] = .loaded(tables)
            procedures[connectionId] = loadedProcedures
            functions[connectionId] = loadedFunctions
            lastLoadDates[connectionId] = Date()
        } catch is CancellationError {
            return
        } catch {
            Self.logger.warning(
                "[schema] grouped load failed connId=\(connectionId, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
            )
            states[connectionId] = .failed(error.localizedDescription)
        }
    }

    private func visibleSchemasForGroupedReload(connectionId: UUID, driver: DatabaseDriver) -> [String]? {
        guard AppSettingsManager.shared.editor.displaySchemasInSidebar else { return nil }
        let schemas = schemasInOrder[connectionId] ?? []
        guard !schemas.isEmpty else { return nil }
        return schemas
    }

    private static func fetchTablesAcrossSchemas(
        driver: DatabaseDriver,
        schemas: [String]
    ) async throws -> [TableInfo] {
        try await withThrowingTaskGroup(of: [TableInfo].self) { group in
            for schema in schemas {
                group.addTask {
                    try await driver.fetchTables(schema: schema)
                }
            }
            var aggregated: [TableInfo] = []
            for try await tables in group {
                aggregated.append(contentsOf: tables)
            }
            return aggregated
        }
    }

    private static func fetchRoutinesAcrossSchemas(
        driver: DatabaseDriver,
        schemas: [String],
        kind: RoutineInfo.Kind
    ) async throws -> [RoutineInfo] {
        try await withThrowingTaskGroup(of: [RoutineInfo].self) { group in
            for schema in schemas {
                group.addTask {
                    switch kind {
                    case .procedure: return try await driver.fetchProcedures(schema: schema)
                    case .function:  return try await driver.fetchFunctions(schema: schema)
                    }
                }
            }
            var aggregated: [RoutineInfo] = []
            for try await routines in group {
                aggregated.append(contentsOf: routines)
            }
            return aggregated
        }
    }

    private static func fetchRoutinesSafely(
        connectionId: UUID,
        kind: RoutineInfo.Kind,
        dedup: OnceTask<UUID, [RoutineInfo]>,
        fetch: @Sendable @escaping () async throws -> [RoutineInfo]
    ) async -> [RoutineInfo] {
        do {
            return try await dedup.execute(key: connectionId, work: fetch)
        } catch is CancellationError {
            return []
        } catch {
            logger.warning(
                "[schema] \(kind.rawValue, privacy: .public) load failed connId=\(connectionId, privacy: .public) error=\(error.localizedDescription, privacy: .public)"
            )
            return []
        }
    }

    private func handleEditorSettingsChange() {
        let now = AppSettingsManager.shared.editor.displaySchemasInSidebar
        guard now != lastDisplaySchemasInSidebar else { return }
        lastDisplaySchemasInSidebar = now

        let sessions = DatabaseManager.shared.activeSessions
        for (connectionId, session) in sessions {
            guard let driver = session.driver else { continue }
            let connection = session.connection
            Task { [weak self] in
                guard let self else { return }
                await self.invalidate(connectionId: connectionId)
                await self.reload(connectionId: connectionId, driver: driver, connection: connection)
            }
        }
    }
}
