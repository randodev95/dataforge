use ratatui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout},
    widgets::{Block, Borders, List, ListItem, Paragraph, Tabs},
    Frame,
};
use crate::{Engine, types::{Model, EnvName}};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AppTab {
    Models,
    Plan,
    Logs,
}

impl AppTab {
    fn from_index(idx: usize) -> Self {
        match idx {
            0 => Self::Models,
            1 => Self::Plan,
            _ => Self::Logs,
        }
    }
}

pub struct App {
    pub engine: Engine,
    pub env: EnvName,
    pub selected_model: usize,
    pub active_tab: usize,
}

impl App {
    pub fn new(engine: Engine, env: &str) -> Self {
        Self {
            engine,
            env: EnvName(env.to_string()),
            selected_model: 0,
            active_tab: 0,
        }
    }

    pub fn get_models(&self) -> Vec<Model> {
        let envs = self.engine.get_environments();
        envs.get(&self.env).cloned().unwrap_or_default()
    }
}

pub fn ui<B: Backend>(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(1),
        ])
        .split(f.area());

    let titles = vec![" [Models] ", " [Plan] ", " [Execution Logs] "];
    let tabs = Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL).title(" DataForge | High-Speed DAG Dashboard "))
        .select(app.active_tab)
        .highlight_style(ratatui::style::Style::default().fg(ratatui::style::Color::Cyan));
    f.render_widget(tabs, chunks[0]);

    let current_tab = AppTab::from_index(app.active_tab);
    match current_tab {
        AppTab::Models => render_models(f, app, chunks[1]),
        AppTab::Plan => render_plan(f, app, chunks[1]),
        AppTab::Logs => render_logs(f, chunks[1]),
    }

    let footer = Paragraph::new(" q: Quit | Tab: Switch View | ↑↓: Select Model | p: Plan | a: Apply ");
    f.render_widget(footer, chunks[2]);
}

fn render_models(f: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
        .split(area);

    let models = app.get_models();
    let items: Vec<ListItem> = models
        .iter()
        .map(|m| ListItem::new(format!(" {} ", m.name.0)))
        .collect();

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(" Model Inventory "))
        .highlight_style(ratatui::style::Style::default().bg(ratatui::style::Color::DarkGray))
        .highlight_symbol(">> ");
    
    f.render_widget(list, chunks[0]);

    if let Some(model) = models.get(app.selected_model) {
        let details = format!(
            "Name: {}\n\nDependencies: {:?}\n\nQuery:\n{}",
            model.name.0, model.deps, model.query
        );
        let detail_panel = Paragraph::new(details)
            .block(Block::default().borders(Borders::ALL).title(" Model Details "));
        f.render_widget(detail_panel, chunks[1]);
    }
}

fn render_plan(f: &mut Frame, _app: &App, area: ratatui::layout::Rect) {
    let p = Paragraph::new(" No plan active. Press 'p' to generate plan for 'dev' -> 'prod'. ")
        .block(Block::default().borders(Borders::ALL).title(" Deployment Plan "));
    f.render_widget(p, area);
}

fn render_logs(f: &mut Frame, area: ratatui::layout::Rect) {
    let p = Paragraph::new(" [INFO] Scheduler initialized.\n [INFO] Connected to DuckDB. ")
        .block(Block::default().borders(Borders::ALL).title(" Execution Logs "));
    f.render_widget(p, area);
}
