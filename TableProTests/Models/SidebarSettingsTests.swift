import Foundation
@testable import TablePro
import Testing

@Suite("SidebarSettings")
struct SidebarSettingsTests {
    @Test("default has displaySchemas off")
    func defaultIsOff() {
        #expect(SidebarSettings.default.displaySchemas == false)
    }

    @Test("Codable round-trip preserves displaySchemas")
    func codableRoundTrip() throws {
        let original = SidebarSettings(displaySchemas: true)
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(SidebarSettings.self, from: data)
        #expect(decoded.displaySchemas == true)
    }

    @Test("decoding payload without displaySchemas defaults to false")
    func legacyPayloadDecodesOff() throws {
        let legacyJson = "{}".data(using: .utf8)!
        let decoded = try JSONDecoder().decode(SidebarSettings.self, from: legacyJson)
        #expect(decoded.displaySchemas == false)
    }
}
