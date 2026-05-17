import SwiftUI

struct SidebarSettingsView: View {
    @Binding var settings: SidebarSettings

    var body: some View {
        Form {
            Section("Schemas") {
                Toggle("Display schemas as collapsible sections", isOn: $settings.displaySchemas)
            }
        }
        .formStyle(.grouped)
        .scrollContentBackground(.hidden)
    }
}

#Preview {
    SidebarSettingsView(settings: .constant(.default))
        .frame(width: 450, height: 200)
}
