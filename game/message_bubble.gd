extends HBoxContainer
class_name MessageBubble

var panel: PanelContainer
var text_label: Label

const SPEAKER_USER: String = "User"
const SPEAKER_PIPER: String = "Piper"
const SPEAKER_MASON: String = "Mason"
const SPEAKER_VERA: String = "Vera"
const SPEAKER_TESS: String = "Tess"
const SPEAKER_SYSTEM: String = "System"

var _user_style: StyleBoxFlat
var _piper_style: StyleBoxFlat
var _mason_style: StyleBoxFlat
var _vera_style: StyleBoxFlat
var _tess_style: StyleBoxFlat
var _system_style: StyleBoxFlat

func _ready() -> void:
    _bind_nodes()
    _build_styles()


func configure(role: String, text: String) -> void:
    _bind_nodes()
    if _user_style == null:
        _build_styles()

    text_label.text = text

    # Left/right alignment and style are role-driven so the caller only passes data.
    if role == SPEAKER_USER:
        alignment = BoxContainer.ALIGNMENT_END
        panel.add_theme_stylebox_override("panel", _user_style)
    elif role == SPEAKER_PIPER:
        alignment = BoxContainer.ALIGNMENT_BEGIN
        panel.add_theme_stylebox_override("panel", _piper_style)
    elif role == SPEAKER_MASON:
        alignment = BoxContainer.ALIGNMENT_BEGIN
        panel.add_theme_stylebox_override("panel", _mason_style)
    elif role == SPEAKER_VERA:
        alignment = BoxContainer.ALIGNMENT_BEGIN
        panel.add_theme_stylebox_override("panel", _vera_style)
    elif role == SPEAKER_TESS:
        alignment = BoxContainer.ALIGNMENT_BEGIN
        panel.add_theme_stylebox_override("panel", _tess_style)
    else:
        alignment = BoxContainer.ALIGNMENT_BEGIN
        panel.add_theme_stylebox_override("panel", _system_style)


func _bind_nodes() -> void:
    if panel == null:
        panel = get_node("Panel") as PanelContainer
    if text_label == null:
        text_label = get_node("Panel/Text") as Label


func _build_styles() -> void:
    _user_style = _new_style(Color(0.24, 0.49, 0.92, 1.0), Color(0.85, 0.91, 1.0, 1.0))
    _piper_style = _new_style(Color(0.20, 0.43, 0.74, 1.0), Color(0.88, 0.93, 0.99, 1.0))
    _mason_style = _new_style(Color(0.27, 0.55, 0.31, 1.0), Color(0.89, 0.96, 0.89, 1.0))
    _vera_style = _new_style(Color(0.71, 0.43, 0.15, 1.0), Color(0.99, 0.93, 0.85, 1.0))
    _tess_style = _new_style(Color(0.50, 0.35, 0.67, 1.0), Color(0.94, 0.90, 0.99, 1.0))
    _system_style = _new_style(Color(0.45, 0.45, 0.50, 1.0), Color(0.93, 0.93, 0.95, 1.0))


func _new_style(border: Color, fill: Color) -> StyleBoxFlat:
    var style: StyleBoxFlat = StyleBoxFlat.new()
    style.bg_color = fill
    style.corner_radius_top_left = 12
    style.corner_radius_top_right = 12
    style.corner_radius_bottom_left = 12
    style.corner_radius_bottom_right = 12
    style.content_margin_left = 12
    style.content_margin_top = 8
    style.content_margin_right = 12
    style.content_margin_bottom = 8
    style.border_width_left = 1
    style.border_width_top = 1
    style.border_width_right = 1
    style.border_width_bottom = 1
    style.border_color = border
    return style
